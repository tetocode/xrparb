import coinlib
import coinlib.utils.config

from coinarb.agents import agent


class Agent(agent.Agent):
    def __init__(self, *args, **kwargs):
        super().__init__('quoinex', *args, **kwargs)

        self.user_id = coinlib.utils.config.load()['quoinex']['user_id']
        self.client = coinlib.quoinex.StreamClient()
