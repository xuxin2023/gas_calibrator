# V1 Calibration Audit

- 生成时间: 2026-04-13T15:00:25+08:00
- 当前分支: `codex/v1-v2-decouple-367a108`
- HEAD: `367a1089ebaca1388dbb9d11648f74513316e502`
- 输出目录: `audit/v1_calibration_audit`

## 使用命令/脚本

- `python tools/audit_v1_calibration.py`
- `python tools/run_v1_online_acceptance.py --output-dir audit/v1_calibration_acceptance_online`
- `python -m pytest -q tests/test_audit_v1_trace_check.py`
- `python -m pytest -q tests/test_runner_v1_writeback_safety.py`
- `python -m pytest -q tests/test_v1_writeback_fault_injection.py`
- `python -m pytest -q tests/test_v1_online_acceptance_tool.py`
- `git log --since="2026-04-03 00:00:00" --stat --patch --unified=0`
- `git grep -n -I -E "V1|校准|标定|calibration|cali|zero|span|CO2|H2O|SENCO|GETCO|MODE|READDATA|point|save|store|insert|db|report|serial|protocol|气路|流程|step|状态机|coefficient|coefficients|readback|writeback|delivery|short_verify" -- . ":(exclude)audit/**"`

## 如何重新生成

1. 在仓库根目录运行 `python tools/audit_v1_calibration.py`。
2. 如果只想重跑只读 trace 检查，可运行 `python -m pytest -q tests/test_audit_v1_trace_check.py`。
3. 需要保留当前 git status 快照时，可使用 `python tools/audit_v1_calibration.py --status-override-file audit/v1_calibration_audit/raw/git_status.txt`。

## 最近 30 个 commit

- `367a1089ebaca1388dbb9d11648f74513316e502` | 2026-04-13 13:38:31 +0800 | chore: sync 2026-04-13 13:38:30
- `063c23014469baa43cbe1a058caf7ea85dbf2e14` | 2026-04-13 13:33:32 +0800 | chore: sync 2026-04-13 13:33:31
- `7036ca7439863859ec96107bd5addcd74aa019c4` | 2026-04-13 13:28:36 +0800 | chore: sync 2026-04-13 13:28:33
- `2ae4bfba168dc9e4ce7be24e74215b9aa2f14e42` | 2026-04-13 13:13:44 +0800 | chore: sync 2026-04-13 13:13:37
- `f41b7b20c35a5051943fecd35bdaf62c05ae8d34` | 2026-04-12 23:38:39 +0800 | chore: sync 2026-04-12 23:38:38
- `bef6db703cf0c33db4731734a606db033cc598dc` | 2026-04-12 22:03:30 +0800 | chore: sync 2026-04-12 22:03:29
- `42c1e838aa5bc30a37fa035902967ae86ca7bb95` | 2026-04-12 21:58:30 +0800 | chore: sync 2026-04-12 21:58:30
- `fb152b44491920315c86b692ec44a50b9e033a58` | 2026-04-12 21:53:30 +0800 | chore: sync 2026-04-12 21:53:30
- `b9894d22c4754ca681b73bf43d2e1036e520b57b` | 2026-04-12 21:43:32 +0800 | chore: sync 2026-04-12 21:43:30
- `2b472cb8949ea97546ab856b181379ff0ca9edee` | 2026-04-12 21:38:30 +0800 | chore: sync 2026-04-12 21:38:30
- `f4e6b0a6efc38cb4f2ea046d78603d29df7d18ae` | 2026-04-12 21:33:32 +0800 | chore: sync 2026-04-12 21:33:30
- `0b0238add2bfa0723d42effa5b0b9148f7f443f1` | 2026-04-12 21:13:31 +0800 | chore: sync 2026-04-12 21:13:31
- `dac8104b44e9a4772b8827684c79d8ab0f59430f` | 2026-04-12 21:03:31 +0800 | chore: sync 2026-04-12 21:03:30
- `65d5c7c74e8f5e9d30a3032b2c3a3e2dc2250055` | 2026-04-12 20:23:50 +0800 | chore: sync 2026-04-12 20:23:47
- `81cdb3bdc6844c931490ac0b62432823b9c8e182` | 2026-04-12 20:18:41 +0800 | chore: sync 2026-04-12 20:18:41
- `b5401b5b2fc1e557fcf4e1d4245b9b6269194eb1` | 2026-04-12 20:13:46 +0800 | chore: sync 2026-04-12 20:13:43
- `0bda405286a42b87eb8a9f2e78c07e9bd0a823e1` | 2026-04-12 19:53:30 +0800 | chore: sync 2026-04-12 19:53:29
- `0b898f573562809bf67f79a96f9bd1d73b430285` | 2026-04-12 19:48:44 +0800 | chore: sync 2026-04-12 19:48:43
- `87ee88577a1b1d26839d6da11639099438a90cfe` | 2026-04-12 19:28:31 +0800 | chore: sync 2026-04-12 19:28:31
- `95cca5a9ae42258e477064d606c0f8f52a23a5ce` | 2026-04-12 19:23:30 +0800 | chore: sync 2026-04-12 19:23:30
- `86ff005dff77bc4f60226aa796a0998f5f11f038` | 2026-04-12 19:18:33 +0800 | chore: sync 2026-04-12 19:18:32
- `18ca560637f82c5f8f360f2e6e7d5ad62c315c2c` | 2026-04-12 19:13:32 +0800 | chore: sync 2026-04-12 19:13:30
- `cce4d09488f53c485541acb3eb1d9a9b55aaa721` | 2026-04-12 19:03:31 +0800 | chore: sync 2026-04-12 19:03:30
- `d4b31f47084523079f090dad45cbbdb89cbf99a0` | 2026-04-12 18:58:32 +0800 | chore: sync 2026-04-12 18:58:31
- `5971675a2fdea403c68a40750f25678a70473f9e` | 2026-04-12 18:53:37 +0800 | chore: sync 2026-04-12 18:53:30
- `a28dbe65f258dcb67f8a2a91657154aff8cfbe98` | 2026-04-12 18:38:30 +0800 | chore: sync 2026-04-12 18:38:30
- `7522a84d085c9d01505451b169f5e2ee35d7ae99` | 2026-04-12 18:33:30 +0800 | chore: sync 2026-04-12 18:33:30
- `daf98ac2bf008e4b806ab684647c8c9bf266f00a` | 2026-04-12 17:23:30 +0800 | chore: sync 2026-04-12 17:23:30
- `511200275e2b8b43766cb7a0b05e9b0a36370ca7` | 2026-04-12 17:18:30 +0800 | chore: sync 2026-04-12 17:18:30
- `57fb0709bd2595dd315b37e162c037a055600383` | 2026-04-12 17:13:30 +0800 | chore: sync 2026-04-12 17:13:30
