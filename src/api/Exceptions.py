"""
Multiple exceptions are raised when the program is excecuted.
Each exception contains a default HTTPStatusCode, meant to be passed through the exception handling in 
each logic module.

    - NotImplementerError: Raised when a method is not implemented yet
    - NotFoundError: Raised when a requested item is not found
    - MissingParameter: Raised when a required parameter is not provided
    - InvalidParameter: Raised when a provided parameter is not valid
    - InvalidMethodError: Raised when the requested method is not supported
    - ServerError: Raised when an operation fails
"""

class NotImplementedError(Exception):
    def __init__(self, message, code = 501):
        self.code = code
        self.message = f"{self.code}:{message}"
        super().__init__(self.message)

class NotFoundError(Exception):
    def __init__(self, message, code = 404):
        self.code = code
        self.message = f"{self.code}:{message}"
        super().__init__(self.message)
class MissingParameter(Exception):
    def __init__(self, message, code = 400):
        self.code = code
        self.message = f"{self.code}:{message}"
        super().__init__(self.message)

class InvalidParameter(Exception):
    def __init__(self, message, code = 400):
        self.code = code
        self.message = f"{self.code}:{message}"
        super().__init__(self.message)

class InvalidMethodError(Exception):
    def __init__(self, message, code = 405):
        self.code = code
        self.message = f"{self.code}:{message}"
        super().__init__(self.message)

class ServerError(Exception):
    def __init__(self, message, code = 500):
        self.code = code
        self.message = f"{self.code}:{message}"
        super().__init__(self.message)