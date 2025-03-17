import jdy
import asyncio
import pandas as pd

jdy = jdy.JDY()

appId = "67c280b7c6387c4f4afd50ae"
entryId = "67d6db5695b397f95545ab89"

daily_data = 'woxjrqDwAAK82qyCzpjIcBICu7BHvxhg'
df = pd.DataFrame({"Encrypted_Data": [daily_data]})

asyncio.run(jdy.batch_create(app_id=appId, entry_id=entryId, source_data=df))