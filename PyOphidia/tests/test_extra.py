import pytest
from where_func import *


@pytest.mark.parametrize(("expression", "if_true", "if_false"),
                         [("tasmax-1197>0", "1", "2"), ("tasmax-1197>150", True, False), ("tasmax>0", 1.1, 2.2),
                          ("tasmax>1197", 1, 2), ("tasmin>0", 1222222, 12343342342),
                          (5, 1, 0), ("tasmin>-0", True, False)])
def test_run(expression, if_true, if_false):
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
