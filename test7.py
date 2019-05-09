# coding: utf-8

import bisect
import datetime
from functools import reduce

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import gridspec, colors as mcolors
from matplotlib.collections import LineCollection, PolyCollection
from matplotlib.colors import colorConverter
from matplotlib.ticker import FuncFormatter
from pandas import DataFrame
from pymongo import MongoClient


def candlestick(ax,
                opens,
                highs,
                lows,
                closes,
                width=4,
                colorup='g',
                colordown='r',
                alpha=0.75):
    '''绘制K线图'''

    delta = width / 2.

    # 中间的Box
    barVerts = [((i - delta, open), (i - delta, close), (i + delta, close),
                 (i + delta, open))
                for i, open, close in zip(range(len(opens)), opens, closes)]

    # 下影线
    downSegments = [((i, low), (i, min(open, close)))
                    for i, low, high, open, close in zip(
                        range(len(lows)), lows, highs, opens, closes)]

    # 上影线
    upSegments = [((i, max(open, close)), (i, high))
                  for i, low, high, open, close in zip(
                      range(len(lows)), lows, highs, opens, closes)]

    rangeSegments = upSegments + downSegments

    r, g, b = colorConverter.to_rgb(colorup)
    colorup = r, g, b, alpha
    r, g, b = colorConverter.to_rgb(colordown)
    colordown = r, g, b, alpha
    colord = {
        True: colorup,
        False: colordown,
    }
    colors = [colord[open < close] for open, close in zip(opens, closes)]

    useAA = 0,  # use tuple here
    lw = 0.5,  # and here
    rangeCollection = LineCollection(
        rangeSegments,
        colors=((0, 0, 0, 1), ),
        linewidths=lw,
        antialiaseds=useAA,
    )

    barCollection = PolyCollection(
        barVerts,
        facecolors=colors,
        edgecolors=((0, 0, 0, 1), ),
        antialiaseds=useAA,
        linewidths=lw,
    )

    minx, maxx = 0, len(rangeSegments) / 2
    miny = min([low for low in lows])
    maxy = max([high for high in highs])

    corners = (minx, miny), (maxx, maxy)
    ax.update_datalim(corners)
    ax.autoscale_view()

    # add these last
    ax.add_collection(rangeCollection)
    ax.add_collection(barCollection)

    return rangeCollection, barCollection


def volume_overlay(ax,
                   opens,
                   closes,
                   volumes,
                   colorup='g',
                   colordown='r',
                   width=4,
                   alpha=1.0):
    """Add a volume overlay to the current axes.  The opens and closes
    are used to determine the color of the bar.  -1 is missing.  If a
    value is missing on one it must be missing on all

    Parameters
    ----------
    ax : `Axes`
        an Axes instance to plot to
    opens : sequence
        a sequence of opens
    closes : sequence
        a sequence of closes
    volumes : sequence
        a sequence of volumes
    width : int
        the bar width in points
    colorup : color
        the color of the lines where close >= open
    colordown : color
        the color of the lines where close <  open
    alpha : float
        bar transparency

    Returns
    -------
    ret : `barCollection`
        The `barrCollection` added to the axes

    """

    colorup = mcolors.to_rgba(colorup, alpha)
    colordown = mcolors.to_rgba(colordown, alpha)
    colord = {True: colorup, False: colordown}
    colors = [
        colord[open < close] for open, close in zip(opens, closes)
        if open != -1 and close != -1
    ]

    delta = width / 2.
    bars = [((i - delta, 0), (i - delta, v), (i + delta, v), (i + delta, 0))
            for i, v in enumerate(volumes) if v != -1]

    barCollection = PolyCollection(
        bars,
        facecolors=colors,
        edgecolors=((0, 0, 0, 1), ),
        antialiaseds=(0, ),
        linewidths=(0.5, ),
    )

    ax.add_collection(barCollection)
    corners = (0, 0), (len(bars), max(volumes))
    ax.update_datalim(corners)
    ax.autoscale_view()

    # add these last
    return barCollection


def millions(x, pos):
    'The two args are the value and tick position'
    return '%1.1fM' % (x * 1e-6)


def thousands(x, pos):
    'The two args are the value and tick position'
    return '%1.1fK' % (x * 1e-3)


def getMOnthFristDays(start_date, end_date):
    '''获取月份的第一天'''
    dates = [
        datetime.date(m // 12, m % 12 + 1, 1)
        for m in range(start_date.year * 12 + start_date.month - 1,
                       end_date.year * 12 + end_date.month)
    ]
    return np.array(dates)


def getDateIndex(dates, tickdates):
    "找出最接近 tickdate 的日期的 index"
    index = [bisect.bisect_left(dates, tickdate) for tickdate in tickdates]
    return np.array(index)


def getMonthNames(dates, index):
    "取得 X 軸上面日期的表示方式"
    names = []
    for i in index:
        if i == 0:
            if dates[i].day > 15:
                names.append("")
            else:
                names.append(dates[i].strftime("%b'%y"))
        else:
            names.append(dates[i].strftime("%b'%y"))

    return np.array(names)


def drawStockPanel(data, title='', colorup='g', colordown='r'):
    '''绘制股票面板，包含K线图、成交量图和MACD图'''

    dates = data['dates']
    opens = data['opens']
    closes = data['closes']
    lows = data['lows']
    highs = data['highs']
    volumes = data['volumes']
    amounts = data['amounts']

    if not reduce(lambda x, y: x and y,
           map(lambda x: x == len(dates),
               [len(opens), len(closes), len(lows), len(highs),
                len(volumes), len(amounts)])):
        raise ValueError('数据长度不相同')

    if len(dates) == 0:
        raise ValueError('空数据')

    dates = pd.to_datetime(dates)

    start_date = dates.iloc[0]
    end_date = dates.iloc[-1]

    # 每月第一天
    tickdates = getMOnthFristDays(start_date, end_date)
    # 每月第一天在dates中的索引
    tickindex = getDateIndex(dates, tickdates)
    # 每月第一天的名称
    ticknames = getMonthNames(dates, tickindex)

    millionformatter = FuncFormatter(millions)
    thousandformatter = FuncFormatter(thousands)

    fig = plt.figure(figsize=(16, 12))
    fig.subplots_adjust(bottom=0.1)
    fig.subplots_adjust(hspace=0)

    # 网格布局
    gs = gridspec.GridSpec(2, 1, height_ratios=[4, 1])

    ax0 = plt.subplot(gs[0])
    candles = candlestick(
        ax0,
        opens,
        highs,
        lows,
        closes,
        width=1,
        colorup=colorup,
        colordown=colordown)

    last_price = "Date:{}, Open:{}, High:{}, Low:{}, Close:{}, Volume:{}".format(
        dates[-1], opens[-1], highs[-1], lows[-1], closes[-1],
        volumes[-1])
    ax0.text(
        0.99,
        0.97,
        last_price,
        horizontalalignment='right',
        verticalalignment='bottom',
        transform=ax0.transAxes)

    draw_price_ta(ax0, df)

    def format_coord1(x, y):
        "用來顯示股價相關資訊"
        try:
            index = int(x + 0.5)
            if index < 0 or index >= len(dates):
                return ""
            else:
                return 'x=%s, y=%1.1f, price=%1.1f' % (
                    dates[int(x + 0.5)], y, closes[int(x + 0.5)])
        except Exception as e:
            print(e.args)
            return ''

    def format_coord2(x, y):
        "用來顯示 Volume 的相關資訊"
        try:
            index = int(x + 0.5)
            if index < 0 or index >= len(dates):
                return ""
            else:
                return 'x=%s, y=%1.1fM, volume=%1.1fM' % (dates[int(
                    x + 0.5)], y * 1e-6, volumes[int(x + 0.5)] * 1e-6)
        except Exception as e:
            print(e.args)
            return ''

    ax0.set_xticks(tickindex)
    ax0.set_xticklabels(ticknames)
    ax0.format_coord = format_coord1
    ax0.legend(loc='upper left', shadow=True, fancybox=True)
    ax0.set_ylabel('Price($)', fontsize=16)
    if has_chinese_font:
        ax0.set_title(
            title, fontsize=24, fontweight='bold', fontproperties=font)
    else:
        ax0.set_title(title, fontsize=24, fontweight='bold')
    ax0.grid(True)

    ax1 = plt.subplot(gs[1], sharex=ax0)
    vc = volume_overlay(
        ax1,
        opens,
        closes,
        volumes,
        colorup=colorup,
        colordown=colordown,
        width=1)

    ax1.set_xticks(tickindex)
    ax1.set_xticklabels(ticknames)
    ax1.format_coord = format_coord2

    ax1.tick_params(axis='x', direction='out', length=5)
    ax1.yaxis.set_major_formatter(millionformatter)
    ax1.yaxis.tick_right()
    ax1.yaxis.set_label_position("right")
    ax1.set_ylabel('Volume', fontsize=16)
    ax1.grid(True)

    plt.setp(ax0.get_xticklabels(), visible=False)

    cursor0 = Cursor(ax0)
    cursor1 = Cursor(ax1)
    plt.connect('motion_notify_event', cursor0.mouse_move)
    plt.connect('motion_notify_event', cursor1.mouse_move)

    # 使用 cursor 的時候，如果沒有設定上下限，圖形的上下限會跑掉
    yh = highs.max()
    yl = lows.min()
    ax0.set_ylim(yl - (yh - yl) / 20.0, yh + (yh - yl) / 20.0)
    ax0.set_xlim(0, len(dates) - 1)

    plt.show()


if __name__ == '__main__':
    client = MongoClient('127.0.0.1', 27017, username='mjh', password='666666', authSource='stock')
    db = client['stock']

    daily = DataFrame(
        list(db['daily'].find({'ts_code': '000001.SZ'})),
        columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close',
                'pre_close', 'change', 'pct_chg', 'vol', 'amount'])

    data = daily[daily['trade_date'] > '20180901']

    drawStockPanel({
        'dates': pd.to_datetime(data['trade_date']),
        'opens': data['open'],
        'closes': data['close'],
        'lows': data['low'],
        'highs': data['high'],
        'volumes': data['vol'],
        'amounts': data['amount'],
    })
