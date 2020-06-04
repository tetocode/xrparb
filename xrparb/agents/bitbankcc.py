from coinarb.agents import agent


class Agent(agent.Agent):
    def __init__(self, *args, **kwargs):
        super().__init__('bitbankcc', *args, **kwargs)
