import csv
from collections import defaultdict




import sys

def main():
    # usage: python analyze_leaderboard.py [gd_report_Username.csv]
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = 'gd_report_Fuxi66.csv'
        
    # Deduce output name from input
    # e.g. gd_report_Andrea.csv -> leaderboard_Andrea.csv
    if 'gd_report_' in input_file:
        base = input_file.replace('gd_report_', '').replace('.csv', '')
        output_file = f'leaderboard_{base}.csv'
    else:
        output_file = 'leaderboard_custom.csv'

    stats = defaultdict(lambda: {'count': 0, 'last_date': ''})
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except FileNotFoundError:
        print(f"Error: Could not find {input_file}.")
        return

    # Process data
    for row in rows:
        mapper = row['mapper_name']
        date = row['last_updated']
        
        stats[mapper]['count'] += 1
        if date > stats[mapper]['last_date']:
            stats[mapper]['last_date'] = date

    # Convert to list and sort
    leaderboard = []
    for mapper, data in stats.items():
        leaderboard.append({
            'mapper_name': mapper,
            'total_gds': data['count'],
            'last_gd_date': data['last_date']
        })
    
    # Sort by count (desc), then name (asc)
    leaderboard.sort(key=lambda x: (-x['total_gds'], x['mapper_name']))

    # Write output
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fields = ['rank', 'mapper_name', 'total_gds', 'last_gd_date']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        
        for i, entry in enumerate(leaderboard, 1):
            entry['rank'] = i
            writer.writerow(entry)
            
    print(f"Leaderboard generated successfully!")
    print(f"Top 5 GDers:")
    for i in range(min(5, len(leaderboard))):
        entry = leaderboard[i]
        print(f"{entry['rank']}. {entry['mapper_name']}: {entry['total_gds']} GDs")
        
    print(f"\nSaved to: {output_file}")

if __name__ == "__main__":
    main()
