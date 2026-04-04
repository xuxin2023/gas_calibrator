from pathlib import Path

from gas_calibrator.coefficients.fit_amt import fit_amt_eq4, save_fit_report


def _synthetic_samples():
    # Model for order=2: [k0,k1,u1,u2,v1,v2,w1]
    coeff = [1.2, -0.3, 0.002, -1e-6, 0.1, -2e-4, 0.02]
    p0 = 1013.25
    t0 = 273.15
    rows = []
    for t_c, p_hpa, sig in [
        (10.0, 1000.0, 0.80),
        (15.0, 1010.0, 0.90),
        (20.0, 1020.0, 1.10),
        (25.0, 980.0, 1.30),
        (30.0, 990.0, 1.50),
        (35.0, 1005.0, 1.70),
        (40.0, 1015.0, 1.90),
    ]:
        t_k = t_c + 273.15
        x = [
            1.0,
            __import__("math").log(sig),
            t_k,
            t_k**2,
            t_k / sig,
            (t_k**2) / sig,
            (p_hpa - p0) / p0,
        ]
        y = sum(a * b for a, b in zip(coeff, x))
        target = y * p0 * t_k / (p_hpa * t0)
        rows.append(
            {
                "co2_ppm_target": target,
                "co2_signal": sig,
                "chamber_temp_c": t_c,
                "pressure_hpa": p_hpa,
            }
        )
    return rows


def test_fit_amt_eq4_and_save_report(tmp_path: Path) -> None:
    samples = _synthetic_samples()
    result = fit_amt_eq4(
        samples,
        gas="co2",
        target_key="co2_ppm_target",
        signal_keys=("co2_signal",),
        temp_keys=("chamber_temp_c",),
        pressure_keys=("pressure_hpa",),
        order=2,
    )

    assert result.n == len(samples)
    assert result.model == "amt_eq4"
    assert "k0" in result.coeffs and "w1" in result.coeffs
    assert result.stats["rmse"] < 1e-9

    outputs = save_fit_report(result, tmp_path, prefix="co2", include_residuals=True)
    assert outputs["json"].exists()
    assert outputs["csv"].exists()
