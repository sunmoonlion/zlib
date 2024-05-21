import time
import threading

class SnowflakeGenerator:
    '''
    解释：
    数据中心 ID（data_center_id）和机器 ID（machine_id）: 用于区分不同的数据中心和机器，每个占用 5 位，总共 10 位。
    时间戳（timestamp）: 占用 41 位，从自定义的纪元开始计时。
    序列号（sequence）: 在同一毫秒内生成的序列号，占用 12 位。
    主要函数：
    _current_timestamp: 获取当前时间的时间戳（毫秒级）。
    _wait_for_next_millis: 等待直到下一个毫秒，保证同一毫秒内序列号不会重复。
    get_id: 生成唯一 ID。
    锁机制：
    使用 threading.Lock 保证生成 ID 的操作是线程安全的。

    这个实现生成的 ID 是基于时间戳的全局唯一 ID，非常适合分布式系统中需要唯一标识符的场景。
    '''
    def __init__(self, data_center_id, machine_id, sequence=0):
        self.data_center_id_bits = 5
        self.machine_id_bits = 5
        self.sequence_bits = 12
        
        self.max_data_center_id = -1 ^ (-1 << self.data_center_id_bits)
        self.max_machine_id = -1 ^ (-1 << self.machine_id_bits)
        self.max_sequence = -1 ^ (-1 << self.sequence_bits)
        
        self.data_center_id_shift = self.sequence_bits + self.machine_id_bits
        self.machine_id_shift = self.sequence_bits
        self.timestamp_shift = self.sequence_bits + self.machine_id_bits + self.data_center_id_bits
        
        self.epoch = 1288834974657
        
        self.data_center_id = data_center_id
        self.machine_id = machine_id
        self.sequence = sequence
        self.last_timestamp = -1
        
        if self.data_center_id > self.max_data_center_id or self.data_center_id < 0:
            raise ValueError(f"data_center_id must be between 0 and {self.max_data_center_id}")
        
        if self.machine_id > self.max_machine_id or self.machine_id < 0:
            raise ValueError(f"machine_id must be between 0 and {self.max_machine_id}")
        
        self.lock = threading.Lock()
    
    def _current_timestamp(self):
        return int(time.time() * 1000)
    
    def _wait_for_next_millis(self, last_timestamp):
        timestamp = self._current_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._current_timestamp()
        return timestamp
    
    def get_id(self):
        with self.lock:
            timestamp = self._current_timestamp()
            
            if timestamp < self.last_timestamp:
                raise Exception("Clock moved backwards. Refusing to generate id.")
            
            if timestamp == self.last_timestamp:
                self.sequence = (self.sequence + 1) & self.max_sequence
                if self.sequence == 0:
                    timestamp = self._wait_for_next_millis(self.last_timestamp)
            else:
                self.sequence = 0
            
            self.last_timestamp = timestamp
            
            id = ((timestamp - self.epoch) << self.timestamp_shift) | \
                 (self.data_center_id << self.data_center_id_shift) | \
                 (self.machine_id << self.machine_id_shift) | \
                 self.sequence
            
            return id

# Example usage
data_center_id = 1
machine_id = 1
snowflake = SnowflakeGenerator(data_center_id, machine_id)

for _ in range(10):
    print(snowflake.get_id())
