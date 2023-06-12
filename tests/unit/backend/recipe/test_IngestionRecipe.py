import unittest.mock as mock

import pytest

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo  # noqa : E402
    from snapred.backend.recipe.CrystallographicInfoRecipe import CrystallographicInfoRecipe  # noqa: E402
    from snapred.meta.Config import Resource

    def test_good_path():
        """Test success of crystal ingestion recipe with a good path name"""
        goodCIF = Resource.getPath("/inputs/crystalInfo/example.cif")
        try:
            ingestRecipe = CrystallographicInfoRecipe()
            data = ingestRecipe.executeRecipe(goodCIF)
            xtal = data["crystalInfo"]
        except:  # noqa: BLE011 E722
            pytest.fail("valid file failed to open")
        else:
            assert isinstance(xtal, CrystallographicInfo)
            assert xtal.hkl[0] == (1, 1, 1)
            assert xtal.hkl[5] == (4, 0, 0)
            assert xtal.dSpacing[0] == 3.13592994862768
            assert xtal.dSpacing[4] == 1.0453099828758932
            assert data["fSquaredThreshold"] == 541.8942599465485

    def test_failed_path():
        """Test failure of crystal ingestion recipe with a bad path name"""
        fakeCIF = "fake_file.cif"
        ingestRecipe = CrystallographicInfoRecipe()
        with pytest.raises(Exception):  # noqa: PT011
            ingestRecipe.executeRecipe(fakeCIF)

    def test_weak_f_squared_lowest():
        """Test weak fSquared method finds lowest 1%"""
        ingestRecipe = CrystallographicInfoRecipe()
        mock_xtal = mock.Mock()
        mock_xtal.fSquared = list(range(36, 136))
        mock_xtal.multiplicities = [1, 2, 3, 4, 5] * 20
        mock_xtal.dSpacing = [1] * 100

        assert ingestRecipe.findFSquaredThreshold(mock_xtal) == 36

    def test_weak_f_squared_median():
        """Test weak fSquared method finds median of lowest 1%"""
        ingestRecipe = CrystallographicInfoRecipe()
        mock_xtal = mock.Mock()
        mock_xtal.fSquared = list(range(36, 336))
        mock_xtal.multiplicities = [1, 2, 3, 4, 5] * 60
        mock_xtal.dSpacing = [1] * 300

        lowFsq = ingestRecipe.findFSquaredThreshold(mock_xtal)
        assert lowFsq != 36
        assert lowFsq == 41

    def test_weak_f_squared_small():
        """Test weak fSquared method finds an answer with small number of options"""
        ingestRecipe = CrystallographicInfoRecipe()
        mock_xtal = mock.Mock()
        mock_xtal.fSquared = [2, 3, 4, 5]
        mock_xtal.multiplicities = [1, 1, 1, 1]
        mock_xtal.dSpacing = [1, 1, 1, 1]
        assert ingestRecipe.findFSquaredThreshold(mock_xtal) == 2

    def test_weak_f_squared_one():
        """Test weak fSquared method finds an answer with only one option"""
        ingestRecipe = CrystallographicInfoRecipe()
        mock_xtal = mock.Mock()
        mock_xtal.fSquared = [2]
        mock_xtal.multiplicities = [1]
        mock_xtal.dSpacing = [1]

        assert ingestRecipe.findFSquaredThreshold(mock_xtal) == 2

    ## from silicon calibrant, courtesy of Malcolm
    example_xtal_info = [
        [(1, 1, 1), 3.13592994862768, 535.9619564273586, 8],
        [(2, 2, 0), 1.9203570608125202, 1023.9787120274582, 12],
        [(3, 1, 1), 1.6376860040951353, 498.1235946287523, 24],
        [(4, 0, 0), 1.3578975000000002, 951.6868701996577, 6],
        [(3, 3, 1), 1.2460922059340045, 462.95658217953996, 24],
        [(4, 2, 2), 1.108718666000307, 884.4987579059488, 24],
        [(3, 3, 3), 1.0453099828758932, 430.2723245685628, 8],
        [(5, 1, 1), 1.0453099828758932, 430.27232456856234, 24],
        [(4, 4, 0), 0.9601785304062601, 822.0540571007734, 12],
        [(5, 3, 1), 0.9181062796881728, 399.8955418627943, 48],
        [(6, 2, -0), 0.8588097858096985, 764.0178878212722, 24],
        [(5, 3, 3), 0.8283097096328722, 371.6633287118501, 24],
        [(4, 4, 4), 0.78398248715692, 710.0790122848589, 8],
        [(5, 5, 1), 0.7605747301605699, 345.42428071520527, 24],
        [(7, 1, 1), 0.7605747301605699, 345.4242807152059, 24],
        [(6, 4, 2), 0.7258267444795525, 659.9481657756581, 48],
        [(5, 5, 3), 0.7071327869943331, 321.0376824669837, 24],
        [(7, 3, 1), 0.7071327869943332, 321.03768246698314, 48],
        [(8, 0, 0), 0.6789487500000001, 613.3565053686378, 6],
        [(7, 3, 3), 0.663574332271264, 298.37275292395174, 24],
        [(6, 6, -0), 0.64011902027084, 570.054168172224, 12],
        [(8, 2, 2), 0.64011902027084, 570.054168172224, 24],
        [(5, 5, 5), 0.627185989725536, 277.3079440497553, 8],  # this is lowest
        [(7, 5, 1), 0.627185989725536, 277.30794404975575, 48],
        [(8, 4, -0), 0.6072702232954043, 529.8089313574962, 24],
        [(7, 5, 3), 0.5961944569174021, 257.73028897414855, 48],
        [(9, 1, 1), 0.5961944569174022, 257.7302889741486, 24],
        [(6, 6, 4), 0.5790094394749851, 492.4049667879426, 24],
        [(9, 3, 1), 0.5693853436290405, 239.53479617150862, 48],
        [(8, 4, 4), 0.5543593330001535, 457.64168357107144, 24],
        [(7, 5, 5), 0.5458953346983784, 222.6238864097245, 24],
        [(7, 7, 1), 0.5458953346983784, 222.62388640972324, 24],
        [(9, 3, 3), 0.5458953346983784, 222.62388640972318, 24],
        [(8, 6, 2), 0.5326112192276046, 425.33265232468636, 48],
        [(10, 2, 0), 0.5326112192276046, 425.33265232468636, 24],
        [(7, 7, 3), 0.5250916246535536, 206.90686944991165, 24],
        [(9, 5, 1), 0.5250916246535536, 206.90686944991126, 48],
        [(9, 5, 3), 0.5064983791390006, 192.29945769059668, 48],
    ]

    def test_example_xtal_fsq():
        """Test weak fSquared method finds an answer in example data"""

        hkl = [col[0] for col in example_xtal_info]
        d = [col[1] for col in example_xtal_info]
        fsq = [col[2] for col in example_xtal_info]
        mul = [col[3] for col in example_xtal_info]

        xtalInfo = CrystallographicInfo(hkl=hkl, dSpacing=d, fSquared=fsq, multiplicities=mul)

        ingestRecipe = CrystallographicInfoRecipe()

        lowF = example_xtal_info[22][2]
        lowM = example_xtal_info[22][3]
        lowD = example_xtal_info[22][1]
        lowFsq = lowF * lowM * (lowD**4)

        assert ingestRecipe.findFSquaredThreshold(xtalInfo) == lowFsq
