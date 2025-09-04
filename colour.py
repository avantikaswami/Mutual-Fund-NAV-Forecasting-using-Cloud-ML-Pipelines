import logging
import sys
import threading

class ColoredFormatter(logging.Formatter):
    """Custom formatter to add colors to log levels"""
    
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
    }
    
    RESET = '\033[0m'           # Reset to default
    BOLD = '\033[1m'            # Bold text
    DIM = '\033[2m'             # Dim text
    
    def format(self, record):
        original_format = super().format(record)
        color = self.COLORS.get(record.levelname, '')
        if color:
            colored_level = f"{color}{self.BOLD}{record.levelname}{self.RESET}"
            colored_format = original_format.replace(record.levelname, colored_level)
            if hasattr(record, 'asctime'):
                colored_format = colored_format.replace(
                    record.asctime, 
                    f"{self.DIM}{record.asctime}{self.RESET}"
                )
        
            if record.name:
                colored_format = colored_format.replace(
                    record.name,
                    f"\033[34m{record.name}{self.RESET}"
                )
            
            return colored_format
        
        return original_format


class SingletonColoredLogger:
    """
    Thread-safe singleton colored logger class
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, name=None, level=logging.INFO, log_file=None):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, name=None, level=logging.INFO, log_file=None):
        if self._initialized:
            return
        
        self.name = name or "Single"
        self.level = level
        self.log_file = log_file
        

        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(level)
        

        self.logger.handlers.clear()
        

        self.colored_formatter = ColoredFormatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        self.file_formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        

        self.console_handler = logging.StreamHandler(sys.stdout)
        self.console_handler.setFormatter(self.colored_formatter)
        self.console_handler.setLevel(level)
        self.logger.addHandler(self.console_handler)
        

        if log_file:
            self.file_handler = logging.FileHandler(log_file)
            self.file_handler.setFormatter(self.file_formatter)
            self.file_handler.setLevel(level)
            self.logger.addHandler(self.file_handler)
        else:
            self.file_handler = None
        
        self._initialized = True
    
    @classmethod
    def get_instance(cls, name=None, level=logging.INFO, log_file=None):
        """Get the singleton instance"""
        return cls(name, level, log_file)
    
    @classmethod
    def reset_instance(cls):
        """Reset the singleton instance (useful for testing)"""
        with cls._lock:
            if cls._instance:

                if hasattr(cls._instance, 'logger'):
                    cls._instance.logger.handlers.clear()
            cls._instance = None
    
    def reconfigure(self, name=None, level=None, log_file=None):
        """Reconfigure the logger settings"""
        if name:
            self.name = name
            self.logger.name = name
        
        if level:
            self.level = level
            self.logger.setLevel(level)
            self.console_handler.setLevel(level)
            if self.file_handler:
                self.file_handler.setLevel(level)
        
        if log_file and not self.file_handler:
            self.file_handler = logging.FileHandler(log_file)
            self.file_handler.setFormatter(self.file_formatter)
            self.file_handler.setLevel(self.level)
            self.logger.addHandler(self.file_handler)
        elif log_file and self.file_handler:

            self.logger.removeHandler(self.file_handler)
            self.file_handler.close()
            self.file_handler = logging.FileHandler(log_file)
            self.file_handler.setFormatter(self.file_formatter)
            self.file_handler.setLevel(self.level)
            self.logger.addHandler(self.file_handler)
    
    def debug(self, message, **kwargs):
        """Log debug message"""
        self.logger.debug(message, **kwargs)
    
    def info(self, message, **kwargs):
        """Log info message"""
        self.logger.info(message, **kwargs)
    
    def warning(self, message, **kwargs):
        """Log warning message"""
        self.logger.warning(message, **kwargs)
    
    def error(self, message, **kwargs):
        """Log error message"""
        self.logger.error(message, **kwargs)
    
    def critical(self, message, **kwargs):
        """Log critical message"""
        self.logger.critical(message, **kwargs)
    
    def exception(self, message, **kwargs):
        """Log exception with traceback"""
        self.logger.exception(message, **kwargs)
    
    def success(self, message):
        """Log success message (custom green message)"""
        print(f"\033[32m\033[1m✓ SUCCESS:\033[0m \033[32m{message}\033[0m")
    
    def alert(self, message):
        """Log alert message (bright yellow with warning symbol)"""
        print(f"\033[93m\033[1m⚠ ALERT:\033[0m \033[93m{message}\033[0m")
    
    def start(self, message):
        """Log start message (bright blue with rocket symbol)"""
        print(f"\033[96m\033[1m✓ START:\033[0m \033[96m{message}\033[0m")
    
    def progress(self, completed, total, message="Processing", show_bar=True, bar_length=30):
        """
        Display a progress indicator with optional progress bar
        
        Args:
            completed (int): Number of completed items
            total (int): Total number of items
            message (str): Progress message
            show_bar (bool): Whether to show visual progress bar
            bar_length (int): Length of the progress bar
        """
        if total == 0:
            return
            
        percentage = (completed / total) * 100
        
        if show_bar:
        
            filled_length = int(bar_length * completed / total)
            bar = '█' * filled_length + '░' * (bar_length - filled_length)
            

            if percentage < 50:
                bar_color = '\033[91m'  # Red
            elif percentage < 80:
                bar_color = '\033[93m'  # Yellow
            else:
                bar_color = '\033[92m'  # Green
            
            progress_line = f"\r\033[94m{message}:\033[0m {bar_color}[{bar}]\033[0m {completed}/{total} ({percentage:.1f}%)"
        else:
            progress_line = f"\r\033[94m{message}:\033[0m {completed}/{total} ({percentage:.1f}%)"
        
        print(progress_line, end="", flush=True)
        
        if completed >= total:
            print()  
    
    def progress_finish(self, message="Completed"):
        """Finish progress display with a completion message"""
        print(f"\n\033[92m\033[1m✓ {message}!\033[0m")
    
    def header(self, message):
        """Log header message (bold blue)"""
        print(f"\n\033[34m\033[1m{'='*50}")
        print(f"{message}")
        print(f"{'='*50}\033[0m\n")
    
    def separator(self):
        """Print a separator line"""
        print(f"\033[90m{'-'*100}\033[0m")


def get_logger(name=None, level=logging.INFO, log_file=None):
    """Get the singleton logger instance"""
    return SingletonColoredLogger.get_instance(name, level, log_file)


def reset_logger():
    """Reset the singleton logger instance"""
    SingletonColoredLogger.reset_instance()
