# -*- coding: utf-8 -*-
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Union

class KGLogger:
    def __init__(self, name, log_dir = "logs"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # 创建日志目录
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        
        # 设置日志格式
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # 文件处理器（带轮转）
        file_handler = RotatingFileHandler(filename=log_path / f"{name}.log", maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
    
    def info(self, message):
        self.logger.info(message)
        
    def error(self, message):
        self.logger.error(message)
        
    def warning(self, message):
        self.logger.warning(message)
        
    def debug(self, message):
        self.logger.debug(message) 