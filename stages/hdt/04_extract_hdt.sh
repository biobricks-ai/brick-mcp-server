#!/usr/bin/env bash

mkdir -p tmp/hdt

while IFS= read -r brick; do
    # Skip empty lines and comments
    [[ -z "$brick" || "$brick" =~ ^[[:space:]]*# ]] && continue

    # Iterate through each asset for this brick
    while read -r asset_line; do
        asset_name=$(echo -e "$asset_line" | awk '{print $1}' | sed 's/:$//')
        asset_path=$(echo -e "$asset_line" | awk '{print $2}')

        if [[ "$asset_path" != *.hdt ]]; then
            continue
        fi

        mkdir -p "tmp/hdt/$brick"

        final_path=$(readlink "$asset_path")

        echo "Starting server for $brick/$asset_name"
        qendpoint-manage start --hdt-file "$final_path" > /dev/null 2>&1
        sleep 3
        python3 <<EOF
import sys, json

print("Running queries")
types = dict($(qendpoint-manage query -t "$final_path" -f stages/type.rq --format json))['results']['bindings']
labels = dict($(qendpoint-manage query -t "$final_path" -f stages/label.rq --format json))['results']['bindings']
preds = dict($(qendpoint-manage query -t "$final_path" -f stages/pred.rq --format json))['results']['bindings']

print("Extracting values from query response")
type_list = [type["type"]["value"] for type in types]
label_list = [label["label"]["value"] for label in labels]
pred_list = [pred["pred"]["value"] for pred in preds]

sample = {
    "entity_types": type_list,
    "labels": label_list,
    "predicates": pred_list,
}

print("Creating final metadata")
metadata = {
    "brick": "$brick",
    "asset": "$asset_name",
    "format": "hdt",
    "info": sample,
}

print("Writing to 'tmp/hdt/$brick/$asset_name.json'")
if len(metadata):
    with open("tmp/hdt/$brick/$asset_name.json", "w") as f_out:
        json.dump(metadata, f_out, indent=2, default=str)
EOF
        
        echo "Stopping $final_path and cleaning up stopped instances"
        qendpoint-manage stop -t "$final_path"
        qendpoint-manage cleanup

    done < <(biobricks assets "$brick")

    combined_file="tmp/hdt/${brick}.json"
    shopt -s nullglob
    json_files=(tmp/hdt/"$brick"/*.json)
    shopt -u nullglob
    if [ ${#json_files[@]} -gt 0 ]; then
        echo "[" > "$combined_file"
        first=1
        for f in "${json_files[@]}"; do
            if [ $first -eq 1 ]; then
                cat "$f" >> "$combined_file"
                first=0
            else
                echo "," >> "$combined_file"
                cat "$f" >> "$combined_file"
            fi
        done
        echo "]" >> "$combined_file"
    fi

done < list/bricks.txt
