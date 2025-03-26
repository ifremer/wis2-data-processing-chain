import json
import argparse
from pywis_pubsub.kpi import WNMKeyPerformanceIndicators
from pywis_pubsub.ets import WNMTestSuite
from pywis_pubsub.verification import verify_data


def pretty_print_json(data, title="Résultat"):
    """
    Affiche un dictionnaire JSON formaté de manière lisible.

    :param data: Dictionnaire contenant les données JSON à afficher.
    :param title: Titre affiché avant les données.
    """
    print(f"\n🔹 === {title} ===")
    print(json.dumps(data, indent=4, ensure_ascii=False))


def run_ets_tests(data):
    """
    Exécute les tests ETS (Essential Test Suite) et affiche les résultats.

    :param data: Données JSON à tester.
    """
    print("\n🚀 Exécution des tests ETS...")
    ts = WNMTestSuite(data)
    results = ts.run_tests()
    pretty_print_json(results, "Résultats des tests ETS")


def run_kpi_evaluation(data):
    """
    Évalue les Key Performance Indicators (KPI) sur les données JSON.

    :param data: Données JSON à analyser.
    """
    print("\n📊 Évaluation des KPI...")
    kpis = WNMKeyPerformanceIndicators(data)
    results = kpis.evaluate()
    pretty_print_json(results["summary"], "Résumé des KPI")


def run_message_verification(data):
    """
    Vérifie la conformité du message JSON avec les standards attendus.

    :param data: Données JSON à vérifier.
    """
    print("\n🔍 Vérification de la conformité du message...")
    result = verify_data(data)
    if result:
        print("✅ Message valide.")
    else:
        print("❌ Échec de la vérification du message.")


def load_json_file(file_path):
    """
    Charge un fichier JSON et gère les erreurs de lecture.

    :param file_path: Chemin du fichier JSON.
    :return: Dictionnaire JSON chargé ou None en cas d'erreur.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"❌ Erreur : Fichier '{file_path}' introuvable.")
    except json.JSONDecodeError as e:
        print(f"❌ Erreur de format JSON : {e}")
    return None


def main():
    """
    Point d'entrée principal du script.
    Parse les arguments de la ligne de commande et exécute les tests.
    """
    parser = argparse.ArgumentParser(
        description="Valide un fichier JSON en exécutant les tests ETS, les KPI et la vérification du message.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("file_path", help="Chemin du fichier JSON à tester")
    args = parser.parse_args()

    # Charger le fichier JSON
    data = load_json_file(args.file_path)
    if data is None:
        return  # Quitte le script en cas d'erreur

    # Exécuter les validations
    run_ets_tests(data)
    run_kpi_evaluation(data)
    run_message_verification(data)


if __name__ == "__main__":
    main()
