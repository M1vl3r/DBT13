from Models.disc import disc
from Models.ensemble import ensemble
from Models.songs import songs

Songs = songs()
print(Songs.getOneRow(1))

Disc = disc()
print(Disc.getOneRow(1))

print(Disc.add())

Ensemble = ensemble()
