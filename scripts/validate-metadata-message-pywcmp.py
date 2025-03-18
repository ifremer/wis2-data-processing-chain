import json
import argparse
import pywcmp.errors
from pywcmp.wcmp2.ets import WMOCoreMetadataProfileTestSuite2
from pywcmp.wcmp2.kpi import WMOCoreMetadataProfileKeyPerformanceIndicators

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
    Exécute les tests ETS (Essential Test Suite) sur les métadonnées JSON.

    :param data: Données JSON à tester.
    """
    print("\n🚀 Exécution des tests ETS...")
    ts = WMOCoreMetadataProfileTestSuite2(data)
    try:
        results = ts.run_tests()
        pretty_print_json(results, "Résultats des tests ETS")
    except pywcmp.errors.TestSuiteError as err:
        print("\n❌ Erreurs détectées lors des tests ETS :")
        print('\n'.join(err.errors))


def run_kpi_evaluation(data):
    """
    Évalue les Key Performance Indicators (KPI) sur les métadonnées JSON.

    :param data: Données JSON à analyser.
    """
    print("\n📊 Évaluation des KPI...")
    kpis = WMOCoreMetadataProfileKeyPerformanceIndicators(data)
    results = kpis.evaluate()
    pretty_print_json(results["summary"], "Résumé des KPI")


def load_json_file(file_path):
    """
    Charge et valide un fichier JSON.

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
        description="Valide le format d'un fichier JSON avec les tests ETS et les KPI.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("file_path", help="Chemin du fichier JSON à tester")
    args = parser.parse_args()

    # Charger le fichier JSON
    data = load_json_file(args.file_path)
    if data is None:
        return  # Quitte le script en cas d'erreur

    # Exécuter les tests
    run_ets_tests(data)
    run_kpi_evaluation(data)


if __name__ == "__main__":
    main()
