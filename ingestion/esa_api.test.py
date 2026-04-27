from astroquery.esa.hubble import ESAHubble

print("Connexion à l'API ESA Hubble")
print("Récupération")

ehst = ESAHubble()
result_table = ehst.query_criteria(instrument_name="ACS")
top_5_results = result_table[:5]

print("\n Données done")
print(result_table.colnames) 

print("\n" + "-" * 70)
print("First 5 rows:")
print(top_5_results)
print("-" * 70)