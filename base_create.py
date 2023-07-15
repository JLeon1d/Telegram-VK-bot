#запустить для создания пустой базы данных

import pandas as pd

df = pd.DataFrame({'id' : [], 'cnt' : [], 'walls' : [], 'last_command' : []})
df.to_csv("base.csv", index=None)