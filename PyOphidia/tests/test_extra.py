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


@pytest.mark.parametrize(("measure", "addend"),
                         [("tasmax", "2"), ("tasmax", 10), ("tasmax", 10.5), ("tasmax", 1000000000000)])
def test_add(measure, addend):
    from PyOphidia import cube
    cube.Cube.setclient(server="ophidialab.cmcc.it", port="11732", username="evachlas", password="F7_Nr9-ro1")
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