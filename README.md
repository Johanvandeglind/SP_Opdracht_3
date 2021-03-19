# SP_Opdracht_3
Business rules voor Recommendation Engine
### create_content_recommendation_table()
De functie kan verschillende tabellen maken en bestaande tabellen legen.
Doordat de functie een aantal inputs heeft is hij overal op toepasbaar
Je kan op category, op prijs op gender enzv een recommendation maken en dan op prijs of op aantal besachikbaar ook nog sorteren.

![plot](./images/Screenshot_content.png)
### get_most_used_bases()
Deze functie geeft van het ingegeven profiel en de ingegeven bases terug wat de meest bekeken values zijn van deze basis voor dit profiel.
Voorbeeld: ik geef een profiel id in met sub_category en gender.
de functie geeft de meest bekeken gender en de meest bekeken sub_category terug van dit profiel. 

### get_similar_profiles()
Deze gebruikt de "get_most_used_bases()" om van de ingegeven functie de top bekeken combinatie te krijgen.
Daarnaast word er van alle profielen een lijst gemaakt (zie afbeelding) van per combinatie de meest bekeken producten.
Elke keer als de functie aangeroepen word dan word er gekeken of de combinatie meest bekeken van het ingegeven profiel al een keer aanbevolen is en dan word deze aanbeveling gebruikt
Als deze aanbeveling niet in de lijst staat dan word de aanbeveling gegenereerd door op te zoeken welke 5 profielen het meest deze combinatie gebruiken/bekeken.
Hier worden 4 producten uitgehaald en terug gegeven. Ook word deze aanbeveling toegevoegd aan de lijst hieronder.
![plot](./images/Screenshot_collab.png)


