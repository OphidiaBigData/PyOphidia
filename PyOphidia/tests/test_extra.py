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


from PyOphidia import cube

cube.Cube.setclient()
random_cube_1 = cube.Cube.randcube(container="mytest", dim="lat|lon|k|l|time", dim_size="4|2|2|2|1", exp_ndim=4,
                                   host_partition="main", measure="tos", measure_type="double", nfrag=8, ntuple=4,
                                   nhost=1)
random_cube_2 = cube.Cube.randcube(container="mytest", dim="lat|lon|time", dim_size="4|2|1", exp_ndim=2,
                                   host_partition="main", measure="tos", measure_type="double", nfrag=4, ntuple=2,
                                   nhost=1)
random_cube_3 = cube.Cube(src_path='/public/data/ecas_training/tasmax_day_CMCC-CESM_rcp85_r1i1p1_20960101-21001231.nc',
                      measure='tasmax',
                      import_metadata='yes',
                      imp_dim='time',
                      imp_concept_level='d', vocabulary='CF', hierarchy='oph_base|oph_base|oph_time',
                      ncores=4,
                      description='Max Temps'
                      )
@pytest.mark.parametrize(("cube"),[(random_cube_1), (random_cube_2), (random_cube_3)])
def test_convert_to_xarray(cube):
    ds = convert_to_xarray(cube)
    print(ds)