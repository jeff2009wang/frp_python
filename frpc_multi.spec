# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['frpc_multi.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[
        'pfrp.constants',
        'pfrp.batch_sender',
        'pfrp.channel_monitor',
        'pfrp.reassembler',
        'pfrp.flow_classifier',
        'pfrp.scheduler',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='frpc_multi',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
