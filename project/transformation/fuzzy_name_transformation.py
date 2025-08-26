import unicodedata
from rapidfuzz import fuzz

def fuzzy_name_transformation(fuzzy_name: str, norm_name_dict: dict, fuzz_score_agg: str) -> str:
    match_norm_name = []
    match_norm_fuzz_score = []
    for norm_name in norm_name_dict:
        fuzzy_names = norm_name_dict[norm_name]
        
        fuzz_scores = []
        count = 0
        for n in fuzzy_names:
            if unicodedata.normalize("NFC", n) in unicodedata.normalize("NFC", fuzzy_name):
                count +=1
                fuzz_score = fuzz.ratio(n, fuzzy_name)
                fuzz_scores.append(fuzz_score)
            
        if count >= 1:
            match_norm_name.append(norm_name)
            if fuzz_score_agg == "mean":
                match_norm_fuzz_score.append(sum(fuzz_scores)/len(fuzz_scores))
            elif fuzz_score_agg == "max":
                match_norm_fuzz_score.append(max(fuzz_scores))

    if len(match_norm_name) == 1:
        return match_norm_name[0]
    elif len(match_norm_name) > 1:
        return match_norm_name[match_norm_fuzz_score.index(max(match_norm_fuzz_score))]
    
    return None