
class ReadError(Exception):
    def __init__(self, ErrorInfo):
        super().__init__(self)
        self.errorinfo = ErrorInfo
    def __str__(self):
        return self.errorinfo

class HandleError(Exception):
    def __init__(self, ErrorInfo):
        super().__init__(self)
        self.errorinfo = ErrorInfo
    def __str__(self):
        return self.errorinfo

class UploadError(Exception):
    def __init__(self, ErrorInfo):
        super().__init__(self)
        self.errorinfo = ErrorInfo
    def __str__(self):
        return self.errorinfo
