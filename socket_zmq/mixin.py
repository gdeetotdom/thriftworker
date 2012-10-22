

class LoopMixin(object):

    app = None

    @property
    def loop(self):
        """Shortcut to loop."""
        return self.app.loop
