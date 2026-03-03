Tagging:

Tag "owner": 
owner = data_team
owner = analytics_team

Tag "domain":
domain = finance
domain = marketing
domain = growth

Tag "environment":
env = dev
env = staging
env = prod

Tag "cost_center" or "project" :
cost_center = FIN_01
cost_center = MKT_02
(different finance teams/marketing teams etc...)

Tag "data_sensitivity" : 
data_sensitivity = PII (RGPD)
data_sensitivity = PCI
(used to apply masking policies on columns with this tag)