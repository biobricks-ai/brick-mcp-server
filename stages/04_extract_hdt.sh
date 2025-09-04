#!/usr/bin/env bash

mkdir -p tmp/hdt

while IFS= read -r brick; do
    # Skip empty lines and comments
    [[ -z "$brick" || "$brick" =~ ^[[:space:]]*# ]] && continue

    # Iterate through each asset for this brick
    while read -r asset_line; do
        asset_name=$(echo "$asset_line" | awk '{print $1}')
        asset_path=$(echo "$asset_line" | awk '{print $2}')

        if [[ "$asset_path" != *.hdt ]]; then
            continue
        fi

        final_path=$(readlink "$asset_path")

        qendpoint-manage start --hdt-file "$final_path"
        python3 <<EOF
import sys, json
types = dict($(qendpoint-manage query -t "$final_path" -f stages/type.rq --format json))['results']['bindings']
labels = dict($(qendpoint-manage query -t "$final_path" -f stages/label.rq --format json))['results']['bindings']
preds = dict($(qendpoint-manage query -t "$final_path" -f stages/pred.rq --format json))['results']['bindings']

type_list = [type["type"]["value"] for type in types]
label_list = [label["label"]["value"] for label in labels]
pred_list = [pred["pred"]["value"] for pred in preds]

sample = {
    "entity_types": type_list,
    "labels": label_list,
    "predicates": pred_list,
}

metadata = {
    "brick": "$brick",
    "asset": "$asset_name",
    "format": "hdt",
    "info": sample,
}

if len(metadata):
    with open("tmp/hdt/$brick.json", "w") as f_out:
        json.dump(metadata, f_out, indent=2, default=str)
EOF
        
        qendpoint-manage -t "$final_path"
        qendpoint-manage cleanup

    done < <(biobricks assets "$brick")
done < list/bricks.txt
