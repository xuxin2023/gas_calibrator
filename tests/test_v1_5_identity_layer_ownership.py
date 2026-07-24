from __future__ import annotations


def test_v1_5_package_owns_sn_identity_initialization_entrypoints():
    from gas_calibrator.v1_5 import sn_identity_initialization as sn_init

    assert callable(sn_init.build_sn_identity_initialization_plan)
    assert callable(sn_init.execute_sn_identity_initialization)
    assert sn_init.AUTHORIZATION_PHRASE == "I_AUTHORIZE_V1_5_SN_IDENTITY_WRITE"


def test_v1_5_package_owns_initialization_database_entrypoints():
    from gas_calibrator.v1_5 import initialization_database as init_db
    from gas_calibrator.v1_5 import import_initialization_database as init_db_cli

    assert callable(init_db.load_v1_5_initialization_bundle)
    assert callable(init_db.import_v1_5_initialization_bundle)
    assert callable(init_db_cli.run_import)


def test_v1_5_package_owns_readiness_event_entrypoints():
    from gas_calibrator.v1_5 import import_readiness_events as readiness_cli
    from gas_calibrator.v1_5 import readiness_event_database as readiness_db

    assert callable(readiness_db.build_v1_5_readiness_event_preview)
    assert callable(readiness_db.import_v1_5_readiness_events)
    assert callable(readiness_cli.run_import)


def test_v2_readiness_event_wrapper_preserves_entrypoint_identity():
    from gas_calibrator.v1_5 import import_readiness_events as readiness_cli
    from gas_calibrator.v1_5 import readiness_event_database as readiness_db
    from gas_calibrator.v2.storage import import_v1_5_readiness_events as wrapper

    assert wrapper.EVENT_TYPE == readiness_db.EVENT_TYPE
    assert wrapper.build_v1_5_readiness_event_preview is readiness_db.build_v1_5_readiness_event_preview
    assert wrapper.import_v1_5_readiness_events is readiness_db.import_v1_5_readiness_events
    assert wrapper.run_import is readiness_cli.run_import


def test_historical_tool_wrappers_still_export_v1_5_sn_entrypoints():
    from gas_calibrator.tools import run_v1_5_sn_identity_initialization as wrapper

    assert callable(wrapper.build_sn_identity_initialization_plan)
    assert callable(wrapper.execute_sn_identity_initialization)
