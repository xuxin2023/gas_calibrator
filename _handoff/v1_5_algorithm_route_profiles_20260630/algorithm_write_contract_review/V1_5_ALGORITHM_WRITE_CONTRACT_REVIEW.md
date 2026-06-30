# V1.5 algorithm write contract review

This is an offline no-write review generated from the V1.5 algorithm route profile.

- CO2 old and new algorithms both preserve the mature route runners.
- CO2 main-chain payloads are reviewed as SENCO1/SENCO3 paired writes.
- New algorithm uses absorption `A=-ln(R/R0(T))/(P_kPa/100)` inside the old seven slots.
- SENCO5 is a separate final affine layer and must not be folded into SENCO1/SENCO3.
- SENCO5 neutralization requires `CLEARSENCO5,YGAS,FFF`.
