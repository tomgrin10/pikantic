# pikantic

[![PyPI](https://img.shields.io/pypi/v/pikantic)](https://pypi.org/project/pikantic/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pikantic)](https://pypi.org/project/pikantic/)
[![PyPI License](https://img.shields.io/pypi/l/pikantic)](https://pypi.org/project/pikantic/)
[![Code Style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black/)

Python library for easy message broker handling using Pydantic

### Basic Usage

```python
from pikantic import Pikantic, IncomingMessage
from pydantic import BaseModel

app = Pikantic(AMQP_URI)


class PersonModel(BaseModel):
    name: str
    age: int


@app.on_rabbit('test_queue')
async def handle_message(msg: IncomingMessage, person: PersonModel):
    print(msg.body)
    print(person.age)


if __name__ == '__main__':
    app.run()
```
