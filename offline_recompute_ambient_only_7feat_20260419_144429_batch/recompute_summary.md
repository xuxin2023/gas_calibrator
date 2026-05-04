# ambient-only offline recompute summary

- git branch: codex/v1-v2-offline-alignment-review-surface
- git head: e829f3d9c51e6ee537029547c1d9f86d609a3d1e
- scanned runs: 136
- processed: 2
- skipped: 127
- failed: 7
- forced model_features: intercept,R,R2,R3,T,T2,RT

## processed runs
- D:\gas_calibrator\logs\run_20260410_132440 -> D:\gas_calibrator\logs\run_20260410_132440\recomputed_ambient_only_7feat_20260419_144556
  - GA01 CO2: a0=-22373.3, a1=11526.4, a2=-8670.75, a3=2249.55, a4=5.4516, a5=-0.0063472, a6=40.8006, a7=16.4238
  - GA01 H2O: a0=-987.79, a1=741.68, a2=-1336.41, a3=665.12, a4=-0.51609, a5=0.000274138, a6=4.83627, a7=0.914133
  - GA02 CO2: a0=-36150.5, a1=46605.4, a2=-33226, a3=7908.63, a4=4.7044, a5=-0.0069138, a6=28.8299, a7=13.6255
  - GA02 H2O: a0=-2948.18, a1=4621.9, a2=-8092.7, a3=4326.4, a4=-1.32919, a5=0.000744454, a6=13.0884, a7=2.22067
  - GA03 CO2: a0=-43909, a1=62725.8, a2=-49092.3, a3=12817.1, a4=3.9538, a5=-0.00573552, a6=40.515, a7=16.5263
  - GA03 H2O: a0=-525.417, a1=4368.17, a2=-8842.51, a3=5468.02, a4=-1.19217, a5=0.000794855, a6=1.26386, a7=-0.0344669
  - GA04 CO2: a0=-36973.7, a1=46459.4, a2=-34813.6, a3=8696.7, a4=4.247, a5=-0.0065914, a6=36.0209, a7=15.5919
  - GA04 H2O: a0=-918.948, a1=3299.79, a2=-5403.55, a3=2749.53, a4=-0.845711, a5=0.000543058, a6=2.6948, a7=0.353923
- D:\gas_calibrator\logs\run_20260411_204123 -> D:\gas_calibrator\logs\run_20260411_204123\recomputed_ambient_only_7feat_20260419_144556
  - GA01 CO2: a0=-21345.7, a1=5157.9, a2=-4062.9, a3=1093.11, a4=7.7506, a5=-0.0115447, a6=41.4669, a7=17.7825
  - GA01 H2O: a0=-1193.32, a1=387.134, a2=-449.46, a3=204.13, a4=0.29955, a5=-0.000275947, a6=5.02316, a7=1.03653
  - GA02 CO2: a0=-27485.8, a1=7994, a2=-5959.4, a3=1505.79, a4=7.8839, a5=-0.0115797, a6=51.1748, a7=22.4436
  - GA02 H2O: a0=-6405.62, a1=2683.3, a2=-3851.3, a3=1980.78, a4=1.23994, a5=-0.00138745, a6=28.6977, a7=5.55437
  - GA03 CO2: a0=-17099.8, a1=3908.3, a2=-3235.8, a3=928.12, a4=7.1113, a5=-0.0103611, a6=34.1746, a7=14.2976
  - GA03 H2O: a0=-421.732, a1=446.043, a2=-713.86, a3=410.8, a4=0.12308, a5=-0.00010237, a6=1.85083, a7=0.310694
  - GA04 CO2: a0=-24407.6, a1=6751.3, a2=-5985.5, a3=1757.6, a4=7.18998, a5=-0.0118041, a6=53.8436, a7=20.5044
  - GA04 H2O: a0=-1620.89, a1=720.63, a2=-867.2, a3=389.586, a4=0.533273, a5=-0.00057597, a6=6.06441, a7=1.33856

## failed runs
- D:\gas_calibrator\logs\run_20260406_180137: recompute_failed:Not enough rows for fit: 0 < 7
- D:\gas_calibrator\logs\run_20260407_185002: recompute_failed:Not enough rows for fit: 0 < 7
- D:\gas_calibrator\logs\run_20260408_192209: recompute_failed:staging_water_summary_empty
- D:\gas_calibrator\logs\run_20260409_113143: recompute_failed:staging_water_summary_empty
- D:\gas_calibrator\logs\run_20260409_133512: recompute_failed:staging_water_summary_empty
- D:\gas_calibrator\logs\run_20260410_084934: recompute_failed:staging_water_summary_empty
- D:\gas_calibrator\logs\run_20260411_165756: recompute_failed:staging_water_summary_empty
