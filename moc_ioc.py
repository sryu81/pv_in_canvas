import asyncio
import random
from caproto.server import PVGroup, ioc_arg_parser, run, pvproperty

class MockIOC(PVGroup):
    """
    Defines a lightweight EPICS IOC with 3 Process Variables (PVs).
    The pvproperty decorator automatically creates Channel Access fields.
    """
    # 1. Define floating-point Process Variables
    room_temp = pvproperty(value=22.5, name='ROOM:TEMP', dtype=float)
    room_humidity = pvproperty(value=45.0, name='ROOM:HUMIDITY', dtype=float)
    
    # 2. Define a string Process Variable for state
    valve_state = pvproperty(value='CLOSED', name='VALVE:STATE', max_length=20)

    @room_temp.startup
    async def room_temp(self, instance, async_lib):
        """Background loop that continuously alters the PV data to simulate hardware."""
        print("EPICS SoftIOC Simulation loop started successfully!")
        
        valve_options = ['OPENING', 'FULLY OPEN', 'CLOSING', 'CLOSED']
        
        while True:
            # Simulate subtle shifting temperature fluctuations
            new_temp = self.room_temp.value + random.uniform(-0.4, 0.4)
            new_temp = max(15.0, min(new_temp, 35.0)) # keep in logical bounds
            await self.room_temp.write(value=new_temp)

            # Simulate subtle shifting humidity fluctuations
            new_hum = self.room_humidity.value + random.uniform(-0.8, 0.8)
            new_hum = max(20.0, min(new_hum, 80.0))
            await self.room_humidity.write(value=new_hum)

            # Periodically randomize the industrial valve state
            if random.random() < 0.1:
                new_state = random.choice(valve_options)
                await self.valve_state.write(value=new_state)

            # Broadcast new values over Channel Access network every 0.5 seconds
            await async_lib.sleep(0.5)

if __name__ == '__main__':
    # Parse standard EPICS arguments and spin up the server
    ioc_options, run_options = ioc_arg_parser(
        desc="Pure-Python Mock EPICS IOC for SCADA testing.",
        default_prefix=""
    )
    
    print("Starting SoftIOC... PVs available on network:")
    print(" -> ROOM:TEMP")
    print(" -> ROOM:HUMIDITY")
    print(" -> VALVE:STATE")
    
    run(MockIOC(prefix="").pvdb, **run_options)
