# Writing a driver for SMV

Smv provides a default driver, `SmvDriver`, as entry point to run an app. Please see `src/main/python/runapp.py` for a 
simplest driver entry script. 

In some advanced use cases, you may need to write your own driver for your SMV application instead of relying entirely on SMV for orchestration. To accomplish this, you should write your own driver with `SmvDriver`. 

Project created by `smv-init` has an example driver created as `src/main/python/appdriver.py`.

## SmvDriver

`SmvDriver` takes care of the boilerplate around parsing SMV's arguments and constructing an SmvApp. The simplest case would be to write a script like

```python
import smv.smvdriver

class MyDriver(smv.smvdriver.SmvDriver):
    def main(self, app, driver_args):
        # your driver logic goes here
        ...

if __name__ == "__main__":
    MyDriver().run()
```

where `app` is the `SmvApp` that `SmvDriver` has constructed for you and `driver_args` are the args you pass to the driver through the CLI.

If you need to control the details of constructing of the `SparkSession`, you can also override `createSpark`.
