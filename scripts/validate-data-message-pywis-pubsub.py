import json
import argparse
from pywis_pubsub.verification import verify_data

def pretty_print_json(data, title="Résultat"):
    """Affiche un dictionnaire JSON formaté proprement."""
    print(f"\n=== {title} ===")
    print(json.dumps(data, indent=4, ensure_ascii=False))


# def run_message_validate(data):
#     """Exécute les tests ETS et affiche les résultats formatés."""
#     ts = WNMTestSuite(data)
#     try:
#         results = ts.run_tests()
#         pretty_print_json(results, "Résultats des tests ETS")
#     except pywcmp.errors.TestSuiteError as err:
#         print("\n=== Erreurs des tests ETS ===")
#         print('\n'.join(err.errors))



def run_message_verify(data):
    """Évalue les KPI et affiche le résumé formaté."""
    verifies = verify_data(data)
    if not verifies:
        print('Verification failed')
    else:
        print('Valid message')


def main():
    """Point d'entrée principal du script."""
    parser = argparse.ArgumentParser(description="Exécute les tests ETS et évalue les KPI d'un fichier JSON.")
    parser.add_argument("file_path", help="Chemin du fichier JSON à tester")
    args = parser.parse_args()

    try:
        with open(args.file_path) as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Erreur lors du chargement du fichier : {e}")
        return

    # run_ets_tests(data)
    run_message_verify(data)


if __name__ == "__main__":
    main()
