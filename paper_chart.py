"""Readable normalized portfolio/benchmark chart."""
import altair as alt
import pandas as pd
import math


def performance_chart(curve):
    frame = curve.copy()
    frame.index.name = '날짜'
    data = frame.reset_index().melt('날짜', var_name='대상', value_name='기준값').dropna()
    data['누적수익률'] = data['기준값'] - 100
    data['표시값'] = data['기준값'].map(lambda v: f'{v:.1f}')
    names = list(curve.columns)
    colors = ['#2563EB', '#EA580C', '#7C3AED'][:len(names)]
    upper = max(120, math.ceil((data['기준값'].max() + 12) / 10) * 10)
    scale = alt.Scale(domain=[60, upper], zero=False, nice=False)
    base = alt.Chart(data).encode(
        x=alt.X('날짜:T', title=None, axis=alt.Axis(format='%m/%d', tickCount=8,
                                                  labelAngle=0, grid=False)),
        y=alt.Y('기준값:Q', title='성과지수 · 시작값 100', scale=scale,
                axis=alt.Axis(values=list(range(60, upper + 1, 20)), grid=True)),
        color=alt.Color('대상:N', scale=alt.Scale(domain=names, range=colors),
                        legend=alt.Legend(title=None, orient='top', direction='horizontal',
                                          symbolStrokeWidth=4, labelFontSize=13)),
        tooltip=[alt.Tooltip('날짜:T', format='%Y-%m-%d'), '대상:N',
                 alt.Tooltip('기준값:Q', format='.2f'),
                 alt.Tooltip('누적수익률:Q', title='누적수익률 (%)', format='+.2f')],
    )
    lines = base.mark_line(strokeWidth=3, clip=True)
    # Sparse labels leave the curves readable on both narrow and wide screens.
    dates = list(curve.index)
    step = max(1, math.ceil(len(dates) / 5))
    selected = set(dates[::step]) | {dates[-1]}
    labels = data[data['날짜'].isin(selected)].copy()
    labels['위치'] = labels['대상'].map({name: i for i, name in enumerate(names)})
    points = alt.Chart(labels).mark_circle(size=48, stroke='white', strokeWidth=1.5).encode(
        x='날짜:T', y=alt.Y('기준값:Q', scale=scale),
        color=alt.Color('대상:N', scale=alt.Scale(domain=names, range=colors), legend=None))
    layers = []
    for i, name in enumerate(names):
        subset = labels[labels['대상'] == name]
        layers.append(alt.Chart(subset).mark_text(
            dy=[-15, 16, -15][i % 3], fontSize=11, fontWeight=600,
            color=colors[i], stroke='white', strokeWidth=0.15,
            align='right').encode(x='날짜:T', y=alt.Y('기준값:Q', scale=scale), text='표시값:N'))
    baseline = alt.Chart(pd.DataFrame({'기준값': [100]})).mark_rule(
        color='#94A3B8', strokeDash=[5, 5], strokeWidth=1).encode(
            y=alt.Y('기준값:Q', scale=scale))
    return alt.layer(baseline, lines, points, *layers).properties(height=440).configure_view(
        stroke=None).configure_axis(
            labelColor='#64748B', titleColor='#475569', labelFontSize=11,
            titleFontSize=12, gridColor='#E2E8F0', domain=False, tickSize=0,
            labelPadding=10).configure(background='#FFFFFF', padding=24)
