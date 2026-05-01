# 摘要口径一致性

- 状态: 一致
- 证据来源: 诊断
- 验收级别: 离线回归
- 风险等级: 低
- 浮点容差: 1e-06
- 浮点字段: ppm_CO2, ppm_H2O, Temp, P, ppm_H2O_Dew
- 精确字段: AnalyzerCoverage, UsableAnalyzers, ExpectedAnalyzers, PointIntegrity, MissingAnalyzers, UnusableAnalyzers, ValidFrames, TotalFrames, FrameStatus
- 失败用例: 无

## reference_on_aligned_rows
- 状态: 一致
- 对比摘要：浮点 5 个在容差内 / 0 个失败，精确字段 9 个一致 / 0 个失败
- 允许偏差：无
- ppm_CO2: v1=400.0 v2=400.0 一致=是 tol=1e-06
- ppm_H2O: v1=6.0 v2=6.0 一致=是 tol=1e-06
- Temp: v1=25.0 v2=25.0 一致=是 tol=1e-06
- P: v1=998.0 v2=998.0 一致=是 tol=1e-06
- ppm_H2O_Dew: v1=7.073535 v2=7.073535 一致=是 tol=1e-06
- AnalyzerCoverage: v1=1/3 v2=1/3 一致=是
- UsableAnalyzers: v1=1 v2=1 一致=是
- ExpectedAnalyzers: v1=3 v2=3 一致=是
- PointIntegrity: v1=閮ㄥ垎缂哄け涓斿惈寮傚父甯? v2=閮ㄥ垎缂哄け涓斿惈寮傚父甯? 一致=是
- MissingAnalyzers: v1=GA03 v2=GA03 一致=是
- UnusableAnalyzers: v1=GA02 v2=GA02 一致=是
- ValidFrames: v1=1 v2=1 一致=是
- TotalFrames: v1=2 v2=2 一致=是
- FrameStatus: v1=部分可用 v2=部分可用 一致=是

## reference_pool_pressure_expansion
- 状态: 一致
- 对比摘要：浮点 5 个在容差内 / 0 个失败，精确字段 9 个一致 / 0 个失败
- 允许偏差：无
- ppm_CO2: v1=400.0 v2=400.0 一致=是 tol=1e-06
- ppm_H2O: v1=6.0 v2=6.0 一致=是 tol=1e-06
- Temp: v1=25.0 v2=25.0 一致=是 tol=1e-06
- P: v1=964.0 v2=964.0 一致=是 tol=1e-06
- ppm_H2O_Dew: v1=9.050208 v2=9.050208 一致=是 tol=1e-06
- AnalyzerCoverage: v1=1/1 v2=1/1 一致=是
- UsableAnalyzers: v1=1 v2=1 一致=是
- ExpectedAnalyzers: v1=1 v2=1 一致=是
- PointIntegrity: v1=瀹屾暣 v2=瀹屾暣 一致=是
- MissingAnalyzers: v1= v2= 一致=是
- UnusableAnalyzers: v1= v2= 一致=是
- ValidFrames: v1=1 v2=1 一致=是
- TotalFrames: v1=2 v2=2 一致=是
- FrameStatus: v1=部分可用 v2=部分可用 一致=是
