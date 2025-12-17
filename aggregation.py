# aggregation.py
import numpy as np
import glob

def aggregate_portfolio(scenario_dir):
    files = glob.glob(f"{scenario_dir}/policy_*.csv")

    portfolio = None

    for f in files:
        data = np.loadtxt(f, delimiter=",", skiprows=1, usecols=[1, 2])

        portfolio = np.vstack([portfolio, data]) if portfolio is not None else data

    return portfolio
