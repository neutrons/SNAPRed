from snapred.backend.dao.state.CalibrantSample.Atom import Atom


def testGetString():
    atom = Atom(atom_type="Si", atom_coordinates=[0.125, 0.125, 0.125], site_occupation_factor=1.0)
    assert atom.getString == "Si 0.125 0.125 0.125 1.0 0.1"
