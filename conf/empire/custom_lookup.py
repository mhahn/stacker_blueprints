

def handler(value, **kwargs):
    """An example of a custom handler.

    The custom handler is registered in a `lookups` map at the top level of the
    stacker config (similar to mappings). The key provided to the map will
    determine how the lookup will be invoked, eg:

        lookups:
            custom: conf.empire.custom_lookup.handler

    You can then reference the lookup within the stacks config:

        stacks:
            - name: sample
              variables:
                  Var1: ${custom someInputValue}

    Which in this case will resolve to:

        Var1: Custom Lookup: someInputValue

    """
    return "Custom Lookup: {}".format(value)
