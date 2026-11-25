import os, sys, unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "aviation-explorer"))
import seed, views


class SeedTest(unittest.TestCase):
    def test_design_doc_reuses_the_explorer_views(self):
        d = seed.design_doc()
        self.assertEqual(d["_id"], "_design/offline")
        src = views.design("airline_accidents")["views"]
        self.assertEqual(sorted(d["views"]), ["by_date", "by_fatalities", "by_phase", "by_registration"])
        for k, v in d["views"].items():
            self.assertEqual(v, src[k])

    def test_in_slice_takes_2006_and_2007_only(self):
        for date, want in [("2005-12-31", False), ("2006-01-01", True), ("2007-12-31", True),
                           ("2008-01-01", False), (None, False)]:
            self.assertEqual(seed.in_slice({"_id": "x", "event_date": date}), want, date)
        self.assertFalse(seed.in_slice({"_id": "y"}))
        self.assertFalse(seed.in_slice({"_id": "_design/explore", "event_date": "2006-05-05"}))

    def test_prepare_drops_rev_only(self):
        self.assertEqual(seed.prepare({"_id": "x", "_rev": "1-a", "n": 1}), {"_id": "x", "n": 1})

    def test_fixture_docs_are_in_the_slice_and_deterministic(self):
        a, b = seed.fixture_docs(300), seed.fixture_docs(300)
        self.assertEqual(a, b)
        self.assertEqual(len({d["_id"] for d in a}), 300)
        for d in a:
            self.assertTrue("2006-01-01" <= d["event_date"] < "2008-01-01")
            self.assertIsInstance(d["total_fatal_injuries"], int)
            self.assertIn("broad_phase_of_flight", d)
            self.assertIn("aircraft_registration", d)
        top = max(d["total_fatal_injuries"] for d in a)
        self.assertEqual(a[0]["_id"], "fixture-0000")
        self.assertEqual(a[0]["total_fatal_injuries"], top)
        self.assertEqual(sum(1 for d in a if d["total_fatal_injuries"] == top), 1)


if __name__ == "__main__":
    unittest.main()
