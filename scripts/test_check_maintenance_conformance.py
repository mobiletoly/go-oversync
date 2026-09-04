#!/usr/bin/env python3
import copy
import importlib.util
import json
from pathlib import Path
import unittest


repo = Path(__file__).resolve().parents[1]
module_path = repo / 'scripts/check-maintenance-conformance.py'
spec = importlib.util.spec_from_file_location('maintenance_conformance_checker', module_path)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class ScenarioReferenceTests(unittest.TestCase):
    def setUp(self):
        corpus = repo / 'conformance/maintenance-conflicts/v2'
        manifest = json.loads((corpus / 'manifest.json').read_text())
        self.artifacts = {name: json.loads((corpus / name).read_text()) for name in manifest['artifacts'] if name.endswith('.json')}
        self.scenarios = json.loads((corpus / 'scenarios.json').read_text())['scenarios']

    def validate(self, scenarios=None, artifacts=None):
        return checker.validate_scenario_contracts(scenarios or self.scenarios, artifacts or self.artifacts)

    def test_current_contract_resolves(self):
        self.validate()

    def test_unknown_artifact_is_rejected(self):
        scenarios = copy.deepcopy(self.scenarios)
        scenarios[-1]['exchanges'][0]['artifact'] = 'missing.json'
        with self.assertRaisesRegex(ValueError, 'unknown scenario artifact'):
            self.validate(scenarios=scenarios)

    def test_missing_or_duplicate_case_is_rejected(self):
        scenarios = copy.deepcopy(self.scenarios)
        scenarios[0]['exchanges'][0]['case'] = 'missing-case'
        with self.assertRaisesRegex(ValueError, 'unresolved scenario case'):
            self.validate(scenarios=scenarios)
        artifacts = copy.deepcopy(self.artifacts)
        artifacts['cases.json'].append(copy.deepcopy(artifacts['cases.json'][0]))
        with self.assertRaisesRegex(ValueError, 'duplicate case IDs'):
            self.validate(artifacts=artifacts)

    def test_standalone_artifact_case_selector_is_rejected(self):
        scenarios = copy.deepcopy(self.scenarios)
        scenarios[-1]['exchanges'][0]['case'] = 'runtime-upgrade.json'
        with self.assertRaisesRegex(ValueError, 'standalone scenario artifact'):
            self.validate(scenarios=scenarios)

    def test_runtime_upgrade_artifact_requires_exact_typed_contract(self):
        runtime = copy.deepcopy(self.artifacts['runtime-upgrade.json'])
        runtime['old_generation'] = '1'
        with self.assertRaisesRegex(ValueError, 'generations are invalid'):
            checker.validate_runtime_upgrade_artifact(runtime)
        runtime = copy.deepcopy(self.artifacts['runtime-upgrade.json'])
        runtime['runtime_rejection_message'] = 'different'
        with self.assertRaisesRegex(ValueError, 'rejection payload is invalid'):
            checker.validate_runtime_upgrade_artifact(runtime)
        runtime = copy.deepcopy(self.artifacts['runtime-upgrade.json'])
        runtime['unexpected'] = True
        with self.assertRaisesRegex(ValueError, 'fields changed'):
            checker.validate_runtime_upgrade_artifact(runtime)


if __name__ == '__main__':
    unittest.main()
