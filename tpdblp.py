from pymongo import MongoClient
import pprint
# attention a bien utiliser vos paramètres de connexion
client = MongoClient("mongodb://root:pass12345@127.0.0.1:27017")
db = client["DBLP"]
publis = db["publis"]

# compter tous les livres (type “Book”)
documents = publis.count_documents({})
pprint.pprint(documents)

# Lister tous les livres (type “Book”)
documents = publis.find({"type": "Book"})
for document_lu in documents:
    pprint.pprint(document_lu)

    # Lister les livres depuis 2014
document5 = publis.find({"type": "Book", "year": {"$gte": 2014}})
for book2014 in document5:
    pprint.pprint(book2014)

    # Liste des publications de l’auteur « Toru Ishida » ;
document3 = publis.find({"authors": "Toru Ishida"})
for authors in document3:
    pprint.pprint(authors)

    # Liste de tous les auteurs distincts
document4 = publis.distinct("authors")
for authorsDistincts in document4:
    pprint.pprint(authorsDistincts)

    # Trier les publications de « Toru Ishida » par titre de livre
document6 = publis.aggregate(
    [{"$match": {"authors": "Toru Ishida"}}, {"$sort": {"title": 1}}])
for auteurLivre in document6:
    pprint.pprint(auteurLivre)

    # Lister les livres depuis 2014
document5 = publis.find({"type": "Book", "year": {"$gte": 2014}})
for book2014 in document5:
    pprint.pprint(book2014)

    # Liste des publications de l’auteur « Toru Ishida » ;
document3 = publis.find({"authors": "Toru Ishida"})
for authors in document3:
    pprint.pprint(authors)

    # Liste de tous les auteurs distincts
document4 = publis.distinct("authors")
for authorsDistincts in document4:
    pprint.pprint(authorsDistincts)

    # Trier les publications de « Toru Ishida » par titre de livre
document6 = publis.aggregate(
    [{"$match": {"authors": "Toru Ishida"}}, {"$sort": {"title": 1}}])
for auteurLivre in document6:
    pprint.pprint(auteurLivre)

    # Compter le nombre de ses publications
document7 = publis.aggregate([{"$match": {"authors": "Toru Ishida"}}, {
                             "$group": {"_id": "null", "total": {"$sum": 1}}}])
for nombrePublication in document7:
    pprint.pprint(nombrePublication)

    # Compter le nombre de publications depuis 2011 et par type
document8 = publis.aggregate([{"$match": {"year": {"$gte": 2011}}}, {
                             "$group": {"_id": "$type", "total": {"$sum": 1}}}])
for nombrePublication2011 in document8:
    pprint.pprint(nombrePublication2011)

    # Compter le nombre de publications par auteur et trier le résultat par ordre croissant
document9 = publis.aggregate([{"$unwind": "$authors"}, {"$group": {
                             "_id": "$authors", "number": {"$sum": 1}}}, {"$sort": {"number": 1}}])
for nombrePublicationAuteurNbCroissant in document9:
    pprint.pprint(nombrePublicationAuteurNbCroissant)
