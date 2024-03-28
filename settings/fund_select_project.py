

mapper = {
'Money Fund Top 1': 'money',
'Money Fund Top 2': 'money',
'Money Fund Top 3': 'money',
'Money Fund Top 4': 'money',
'Money Fund Top 5': 'money',
'Money Fund Top 6': 'money',
'Money Fund Top 7': 'money',
'Money Fund Top 8': 'money',
'Money Fund Top 9': 'money',
'Money Fund Top 10': 'money',
'Bond Fund Top 1': 'bond',
'Bond Fund Top 2': 'bond',
'Bond Fund Top 3': 'bond',
'Bond Fund Top 4': 'bond',
'Bond Fund Top 5': 'bond',
'Bond Fund Top 6': 'bond',
'Bond Fund Top 7': 'bond',
'Bond Fund Top 8': 'bond',
'Bond Fund Top 9': 'bond',
'Bond Fund Top 10': 'bond',
'Risky Fund Top 1': 'risky',
'Risky Fund Top 2': 'risky',
'Risky Fund Top 3': 'risky',
'Risky Fund Top 4': 'risky',
'Risky Fund Top 5': 'risky',
'Risky Fund Top 6': 'risky',
'Risky Fund Top 7': 'risky',
'Risky Fund Top 8': 'risky',
'Risky Fund Top 9': 'risky',
'Risky Fund Top 10': 'risky',
}


most_conservative_weight_bounds = (0, .5)
most_conservative_λ = 3
most_conservative_γ = .1
most_conservative_lower = {'money': 1}
most_conservative_upper = {}

moderate_conservative_weight_bounds = (0, .2)
moderate_conservative_λ = 1
moderate_conservative_γ = .1
moderate_conservative_lower = {}
moderate_conservative_upper = {'risky': 0}

balanced_weight_bounds = (0, .2)
balanced_λ = 1
balanced_γ = 1
balanced_lower = {}
balanced_upper = {}

moderate_risky_weight_bounds = (0, .2)
moderate_risky_λ = 1
moderate_risky_γ = 1
moderate_risky_lower = {'risky': .3}
moderate_risky_upper = {}

most_risky_weight_bounds = (0, .2)
most_risky_λ = .1
most_risky_γ = 1
most_risky_lower = {'risky': .5}
most_risky_upper = {}

extreme_weight_bounds = (0, 1)
extreme_λ = .01
extreme_γ = .01
extreme_lower = {'risky': 1}
extreme_upper = {}
