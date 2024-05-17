class Error(Exception):
    status: str
    message: str

    def __init__(self, status: str, message: str):
        self.status = status
        self.message = message

    @staticmethod
    def from_tonic_error_string(error: str) -> "Error":
        data = error.split(", ")
        status = data[0].split(": ")[1]

        message = data[1].split(": ", 1)[1]
        if message.startswith('"') and message.endswith('"'):
            message = message[1:-1]

        error = Error(status, message)
        return error

    def __str__(self):
        return f"{self.status} | {self.message.capitalize()}"

    def __repr__(self):
        return f"Error(status={self.status!r}, message={self.message!r})"
