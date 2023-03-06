


def ajuste_strings_enderecos(enderecos):
    if isinstance(enderecos, str):
        try:
            endereco = str(enderecos).strip().capitalize().replace(
                "-"," ").replace("'","")
            return endereco
        except:
            return "NotFound"
    else:
        return "NotFound"
    
