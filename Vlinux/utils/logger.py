import logging

# 获取一个名为 'example_logger' 的日志记录器对象
logger = logging.getLogger('example_logger')

# 配置日志记录器的日志级别
logger.setLevel(logging.DEBUG)

# 创建一个控制台处理器并设置日志级别
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# 创建一个文件处理器，将日志写入文件
file_handler = logging.FileHandler('example.log')
file_handler.setLevel(logging.DEBUG)

# 创建一个日志格式器并将其添加到处理器
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# 将处理器添加到日志记录器
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# 记录一些日志消息
logger.debug('This is a debug message')
logger.info('This is an info message')
logger.warning('This is a warning message')
logger.error('This is an error message')
logger.critical('This is a critical message')
