"""
typo_corrector.py — Corrección automática de BOPs mal escritos por drivers.

Jerarquía de reglas (de mayor a menor certeza):

  REGLA 1 — Ruta + Punto exactos en el Excel:
      El driver declaró RUTA X PUNTO N. El Excel dice que en RUTA X PUNTO N
      está el BOP real. Si la distancia entre el typo y ese BOP es <= 2,
      la corrección es prácticamente certera -> confianza EXACTA.

  REGLA 2 — 1 dígito diferente (sin importar ruta/punto):
      Solo hay un candidato plausible -> confianza ALTA.

  REGLA 3 — 2 dígitos diferentes + ruta declarada coincide:
      La ruta acota los candidatos -> confianza ALTA.

  Cualquier otro caso -> None, queda como "No reconocido".
"""

import re


def _distancia(a, b):
    if len(a) != len(b):
        comun = min(len(a), len(b))
        return abs(len(a) - len(b)) + sum(x != y for x, y in zip(a[:comun], b[:comun]))
    return sum(x != y for x, y in zip(a, b))


def _num(valor):
    if valor is None:
        return None
    m = re.search(r'\d+', str(valor))
    return m.group(0) if m else None


def corregir_bop(bop_typo, ruta_declarada, punto_declarado,
                 rutas_csv, bop_to_ruta, bop_por_ruta_punto=None):
    """
    Intenta corregir un BOP no reconocido.

    bop_por_ruta_punto: dict {ruta_num_str: {punto_int: bop_str}}
                        Construido desde el Excel en load_xlsx().
                        Habilita la Regla 1 (maxima certeza).

    Retorna None o dict con: bop_corregido, bop_original, ruta_real,
                              distancia, confianza, tipo
    """
    if bop_typo in bop_to_ruta:
        return None
    if not bop_to_ruta:
        return None

    ruta_num  = _num(ruta_declarada)
    punto_str = _num(punto_declarado)
    punto_num = int(punto_str) if punto_str else None

    # REGLA 1: Ruta + Punto -> lookup directo en el Excel
    if bop_por_ruta_punto and ruta_num and punto_num is not None:
        bop_en_posicion = (bop_por_ruta_punto.get(ruta_num) or {}).get(punto_num)
        if bop_en_posicion and bop_en_posicion != bop_typo:
            dist = _distancia(bop_typo, bop_en_posicion)
            if dist <= 2:
                ruta_real = bop_to_ruta.get(bop_en_posicion, 'RUTA ' + ruta_num)
                return {
                    'bop_corregido': bop_en_posicion,
                    'bop_original':  bop_typo,
                    'ruta_real':     ruta_real,
                    'distancia':     dist,
                    'confianza':     'exacta',
                    'tipo':          'ruta{}_punto{}'.format(ruta_num, punto_num),
                }

    # Candidatos para Reglas 2 y 3
    candidatos = []
    for real_bop, ruta_real in bop_to_ruta.items():
        dist = _distancia(bop_typo, real_bop)
        if dist == 0:
            continue
        ruta_real_num = _num(ruta_real)
        ruta_coincide = bool(ruta_num and ruta_real_num and ruta_num == ruta_real_num)
        candidatos.append({
            'bop': real_bop, 'dist': dist,
            'ruta': ruta_real, 'ruta_coincide': ruta_coincide,
        })

    if not candidatos:
        return None

    candidatos.sort(key=lambda x: (x['dist'], not x['ruta_coincide']))
    mejor = candidatos[0]

    # REGLA 2: 1 digito diferente
    if mejor['dist'] == 1:
        return {
            'bop_corregido': mejor['bop'], 'bop_original': bop_typo,
            'ruta_real': mejor['ruta'], 'distancia': mejor['dist'],
            'confianza': 'alta', 'tipo': 'typo_1dig',
        }

    # REGLA 3: 2 digitos + ruta coincide
    if mejor['dist'] == 2 and mejor['ruta_coincide']:
        return {
            'bop_corregido': mejor['bop'], 'bop_original': bop_typo,
            'ruta_real': mejor['ruta'], 'distancia': mejor['dist'],
            'confianza': 'alta', 'tipo': 'typo_2dig_ruta',
        }

    return None


def corregir_lista_bops(bops, ruta_declarada, punto_declarado,
                        rutas_csv, bop_to_ruta, bop_por_ruta_punto=None):
    """Corre corregir_bop() sobre una lista de BOPs del mismo mensaje."""
    bops_corregidos  = []
    correcciones_log = []
    for bop in bops:
        if bop in bop_to_ruta:
            bops_corregidos.append(bop)
            continue
        resultado = corregir_bop(bop, ruta_declarada, punto_declarado,
                                 rutas_csv, bop_to_ruta, bop_por_ruta_punto)
        if resultado:
            bops_corregidos.append(resultado['bop_corregido'])
            correcciones_log.append(resultado)
        else:
            bops_corregidos.append(bop)
    return bops_corregidos, correcciones_log


if __name__ == '__main__':
    rutas_ej = {
        'RUTA 3':  ['3825942', '3829348', '3825393'],
        'RUTA 12': ['3833993', '3829098', '3831244'],
        'RUTA 9':  ['3831197', '3829366', '3831227'],
    }
    b2r_ej = {b: r for r, bops in rutas_ej.items() for b in bops}
    brp_ej = {
        '3':  {1: '3825942', 2: '3829348', 3: '3825393'},
        '12': {1: '3833993', 2: '3829098', 3: '3831244'},
        '9':  {1: '3831197', 2: '3829366', 3: '3831227'},
    }

    casos = [
        ('3828942', 'RUTA 3',  1,    'dist=2 R3 P1 → EXACTA → 3825942'),
        ('3833393', 'RUTA 12', 1,    'dist=1 R12 P1 → EXACTA → 3833993'),
        ('3831117', 'RUTA 9',  1,    'dist=2 R9 P1 → EXACTA → 3831197'),
        ('3828942', 'RUTA 3',  None, 'sin punto, dist=1 → ALTA'),
        ('3831117', 'RUTA 9',  None, 'sin punto, dist=2+ruta → ALTA'),
        ('9999999', 'RUTA 3',  1,    'muy lejos → None'),
        ('3825942', 'RUTA 3',  1,    'ya reconocido → None'),
    ]

    print('=== Tests typo_corrector ===\n')
    for bop, ruta, punto, desc in casos:
        res = corregir_bop(bop, ruta, punto, rutas_ej, b2r_ej, brp_ej)
        if res:
            print(f'  OK  {bop} R={ruta} P={punto} → {res["bop_corregido"]} '
                  f'[dist={res["distancia"]}, conf={res["confianza"]}, tipo={res["tipo"]}]')
        else:
            print(f'  --  {bop} R={ruta} P={punto} → sin correccion')
        print(f'      {desc}')
