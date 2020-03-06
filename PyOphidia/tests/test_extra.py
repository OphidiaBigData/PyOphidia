import pytest
from extra import *


@pytest.mark.skip(reason="skipping this")
@pytest.mark.parametrize(("expression", "if_true", "if_false"),
                         [("tasmax-1197>0", "1", "2"), ("tasmax-1197>150", True, False), ("tasmax>0", 1.1, 2.2),
                          ("tasmax>1197", 1, 2), ("tasmin>0", 1222222, 12343342342),
                          (5, 1, 0), ("tasmin>-0", True, False)])
def test_where(expression, if_true, if_false):
    from PyOphidia import cube
    cube.Cube.setclient()
    cube = cube.Cube(src_path='/public/data/ecas_training/tasmax_day_CMCC-CESM_rcp85_r1i1p1_20960101-21001231.nc',
                     measure='tasmax',
                     import_metadata='yes',
                     imp_dim='time',
                     imp_concept_level='d', vocabulary='CF', hierarchy='oph_base|oph_base|oph_time',
                     ncores=4,
                     description='Max Temps'
                     )

    cube.info(display=False)
    results = where(cube=cube, expression=expression, if_true=1, if_false=0)

@pytest.mark.skip(reason="skipping this")
@pytest.mark.parametrize(("measure", "addend"),
                         [("tasmax", "2"), ("tasmax", 10), ("tasmax", 10.5), ("tasmax", 1000000000000),
                          ("tasmax", [1 for i in range(0, 1826)]), ("tasmax", [1 for i in range(0, 10)])])
def test_add(measure, addend):
    from PyOphidia import cube
    cube.Cube.setclient()
    cube = cube.Cube(src_path='/public/data/ecas_training/tasmax_day_CMCC-CESM_rcp85_r1i1p1_20960101-21001231.nc',
                     measure='tasmax',
                     import_metadata='yes',
                     imp_dim='time',
                     imp_concept_level='d', vocabulary='CF', hierarchy='oph_base|oph_base|oph_time',
                     ncores=4,
                     description='Max Temps'
                     )

    cube.info(display=False)
    results = add(cube=cube, measure=measure, addend=addend)

@pytest.mark.skip(reason="skipping this")
@pytest.mark.parametrize(("measure", "multiplier"),
                         [("tasmax", "2"), ("tasmax", 10), ("tasmax", 10.5), ("tasmax", 1000000000000),
                          ("tasmax", [1 for i in range(0, 1826)]), ("tasmax", [1 for i in range(0, 10)])])
def test_multiply(measure, multiplier):
    from PyOphidia import cube
    cube.Cube.setclient()
    cube = cube.Cube(src_path='/public/data/ecas_training/tasmax_day_CMCC-CESM_rcp85_r1i1p1_20960101-21001231.nc',
                     measure='tasmax',
                     import_metadata='yes',
                     imp_dim='time',
                     imp_concept_level='d', vocabulary='CF', hierarchy='oph_base|oph_base|oph_time',
                     ncores=4,
                     description='Max Temps'
                     )

    cube.info(display=False)
    results = multiply(cube=cube, measure=measure, multiplier=multiplier)

@pytest.mark.skip(reason="skipping this")
@pytest.mark.parametrize(("type", "tolerance", "lat", "lon"),
                         [("index", 0, ["1:5:1", "2"], ["1:10:1"]), ("index", 0, ["1:-5:3", "2"], ["1:-2"]),
                          ("coord", 2, ["[1:5]", "2"], ["43"]), ("index", 12, ["[1:5:1]", "2"], ["[1:10:1]"]),
                          ("index", 12, ["[1:200:1]", "2"], ["[1:10:1]"])])
def test_select(type, tolerance, lat, lon):
    from PyOphidia import cube

    cube.Cube.setclient()
    cube = cube.Cube(src_path='/public/data/ecas_training/tasmax_day_CMCC-CESM_rcp85_r1i1p1_20960101-21001231.nc',
                     measure='tasmax',
                     import_metadata='yes',
                     imp_dim='time',
                     imp_concept_level='d', vocabulary='CF', hierarchy='oph_base|oph_base|oph_time',
                     ncores=4,
                     description='Max Temps'
                     )
    if type == "coord":
        results = select(cube=cube, type=type, ncores=1, nthreads=1, description='-',
                         display=False, tolerance=tolerance, lat=lat, lon=lon, time=["MAM", "01/11/2013"])
    else:
        results = select(cube=cube, type=type, ncores=1, nthreads=1, description='-',
                         display=False, tolerance=tolerance, lat=lat, lon=lon)


from PyOphidia import cube

cube.Cube.setclient()
random_cube_1 = cube.Cube.randcube(container="mytest", dim="lat|lon|k|l|time", dim_size="4|2|2|2|1", exp_ndim=4,
                                   host_partition="main", measure="tos", measure_type="double", nfrag=8, ntuple=4,
                                   nhost=1)
random_cube_2 = cube.Cube.randcube(container="mytest", dim="lat|lon|time", dim_size="4|2|1", exp_ndim=2,
                                   host_partition="main", measure="tos", measure_type="double", nfrag=4, ntuple=2,
                                   nhost=1)
@pytest.mark.parametrize(("cube", "precision"),[(random_cube_1, 2), (random_cube_2, 5)])
def test_summary(cube, precision):
    summary(cube, precision)