import pandas as pd
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

#从统计学的角度来看，数据可视化主要分为单变量图和双变量图两种。单变量图主要用于展示单个变量的分布情况，包括箱线图、直方图和热力图等。双变量图则用于展示两个变量之间的关系，包括折线图、散点图等。
#虽然单变量图通常只展示一个变量的分布情况，但其实现方式可能涉及到两个维度（变量值和频数或密度）。所以，严格来说，单变量图也是双变量图的一种。但是，为了便于理解，我们还是将其分为两种。
# 变量还是分为离散变量和连续变量。离散变量主要用柱状图和饼状图来展示，连续变量主要用箱线图和直方图，严格说，离散和连续的区分也是认为的区分，有时可以互相转换。


#  一、双变量图
#采用面向对象的方式画折线图图
def draw_line_chart(data, x, y, title, xlim,lim,xlabel, ylabel):
    """
    画折线图
    :param data: 数据
    :param x: x轴数据
    :param y: y轴数据
    :param title: 标题
    ：parm xlim: x轴范围
    ：parm ylim: y轴范围
    :param xlabel: x轴标签
    :param ylabel: y轴标签
    :return:
    """
    fig, ax = plt.subplots(1,2.figsize=(10, 6))
    ax[0].plot(data[x], data[y], marker='o', color='r')
    ax[0].set_title(title)
    ax[0].set_xlim(xlim)
    ax[0].set_ylim(ylim)
    ax[0].set_xlabel(xlabel)
    ax[0].set_ylabel(ylabel)
    plt.show()

#采用面向对象的方式画折线图图:第二种方式
def draw_line_chart(data, x, y, title, xlim,lim,xlabel, ylabel):
    """
    画折线图
    :param data: 数据
    :param x: x轴数据
    :param y: y轴数据
    :param title: 标题
    ：parm xlim: x轴范围
    ：parm ylim: y轴范围
    :param xlabel: x轴标签
    :param ylabel: y轴标签
    :return:
    """
    fig = plt.subplots(figsize=(10, 6))
    ax1 = fig.add_subplot(1, 2, 1)
    ax1.plot(data[x], data[y], marker='o', color='r')
    ax1.set_title(title)
    ax1.set_xlim(xlim)
    ax1.set_ylim(ylim)
    ax1.set_xlabel(xlabel)
    ax1.set_ylabel(ylabel)   
    plt.show()


#采用面向过程的方式画折线图图
def draw_line_chart(data, x, y, title, xlabel, ylabel):
    """
    画折线图
    :param data: 数据
    :param x: x轴数据
    :param y: y轴数据
    :param title: 标题
    :param xlabel: x轴标签
    :param ylabel: y轴标签
    :return:
    """
    plt.figure(figsize=(10, 6))
    plt.plot(data[x], data[y], marker='o')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.show()


def draw_scatter_chart(data, x, y, title, xlabel, ylabel):
    """
    画散点图
    :param data: 数据
    :param x: x轴数据
    :param y: y轴数据
    :param title: 标题
    :param xlabel: x轴标签
    :param ylabel: y轴标签
    :return:
    """
    plt.figure(figsize=(10, 6))
    plt.scatter(data[x], data[y])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.show()

# 二、单变量图

#（1）离散变量

#以下是表示离散变量的饼状图和柱状图的绘制函数
#采用面向对象的方式画柱状图
def draw_bar_chart(data, x, y, title, xlabel, ylabel):
    """
    画柱状图
    :param data: 数据
    :param x: x轴数据
    :param y: y轴数据
    :param title: 标题
    :param xlabel: x轴标签
    :param ylabel: y轴标签
    :return:
    """
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(data[x], data[y])
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    plt.show()

#采用面向过程的方式画柱状图
def draw_bar_chart(data, x, y, title, xlabel, ylabel):
    """
    画柱状图
    :param data: 数据
    :param x: x轴数据
    :param y: y轴数据
    :param title: 标题
    :param xlabel: x轴标签
    :param ylabel: y轴标签
    :return:
    """
    plt.figure(figsize=(10, 6))
    plt.bar(data[x], data[y])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.show()

#采用面向对象的方式画饼图
def draw_pie_chart(data, x, y, title):
    """
    画饼图
    :param data: 数据
    :param x: x轴数据
    :param y: y轴数据
    :param title: 标题
    :return:
    """
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.pie(data[y], labels=data[x], autopct='%1.1f%%')
    ax.set_title(title)
    plt.show()

#采用面向过程的方式画饼状图
def draw_pie_chart(data, x, y, title):
    """
    画饼图
    :param data: 数据
    :param x: x轴数据
    :param y: y轴数据
    :param title: 标题
    :return:
    """
    plt.figure(figsize=(10, 6))
    plt.pie(data[y], labels=data[x], autopct='%1.1f%%')
    plt.title(title)
    plt.show()


#（2）表示连续变量的箱线图和直方图的绘制函数
def draw_box_chart(data, x, y, title, xlabel, ylabel):
    """
    画箱线图
    :param data: 数据
    :param x: x轴数据
    :param y: y轴数据
    :param title: 标题
    :param xlabel: x轴标签
    :param ylabel: y轴标签
    :return:
    """
    plt.figure(figsize=(10, 6))
    plt.boxplot(data[y])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.show()

def draw_hist_chart(data, x, y, title, xlabel, ylabel):
    """
    画直方图
    :param data: 数据
    :param x: x轴数据
    :param y: y轴数据
    :param title: 标题
    :param xlabel: x轴标签
    :param ylabel: y轴标签
    :return:
    """
    plt.figure(figsize=(10, 6))
    plt.hist(data[y], bins=10)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.show()







