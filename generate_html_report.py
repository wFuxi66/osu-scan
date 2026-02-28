import csv




CSS = """
<style>
    body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; padding: 20px; background-color: #f4f4f9; }
    h1 { color: #333; }
    table { width: 100%; border-collapse: collapse; margin-top: 20px; background: white; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
    th { background-color: #66ccff; color: white; padding: 12px; text-align: left; }
    td { padding: 12px; border-bottom: 1px solid #ddd; }
    tr:nth-child(even) { background-color: #f9f9f9; }
    tr:hover { background-color: #f1f1f1; }
    .rank-1 { color: #d4af37; font-weight: bold; }
    .rank-2 { color: #c0c0c0; font-weight: bold; }
    .rank-3 { color: #cd7f32; font-weight: bold; }
</style>
"""

import sys

def main():
    # usage: python generate_html_report.py [leaderboard_Username.csv]
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = 'gder_leaderboard.csv'
        
    # Deduce output name
    # leaderboard_Andrea.csv -> leaderboard_Andrea.html
    # gder_leaderboard.csv -> leaderboard.html
    if 'leaderboard_' in input_file:
         # Check if it is the generic 'gder_leaderboard.csv'
        if input_file == 'gder_leaderboard.csv':
             output_file = 'leaderboard.html'
        else:
             base = input_file.replace('leaderboard_', '').replace('.csv', '')
             output_file = f'leaderboard_{base}.html'
             
    elif input_file == 'gder_leaderboard.csv':
         output_file = 'leaderboard.html'
    else:
         output_file = 'leaderboard_custom.html'
         
    # Extract username for title
    username = "User"
    if 'leaderboard_' in output_file:
         username = output_file.replace('leaderboard_', '').replace('.html', '')
    if output_file == 'leaderboard.html':
         username = 'Fuxi66' # Default for backward compat

    html_content = [f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>osu! GD Leaderboard - {username}</title>
        {CSS}
    </head>
    <body>
        <h1>Guest Difficulty Leaderboard for {username}</h1>
        <table>
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Mapper</th>
                    <th>Total GDs</th>
                    <th>Last Contribution</th>
                </tr>
            </thead>
            <tbody>
    """]

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rank_class = f"class='rank-{row['rank']}'" if int(row['rank']) <= 3 else ""
                html_content.append(f"""
                <tr>
                    <td {rank_class}>#{row['rank']}</td>
                    <td {rank_class}>{row['mapper_name']}</td>
                    <td>{row['total_gds']}</td>
                    <td>{row['last_gd_date']}</td>
                </tr>
                """)
    except FileNotFoundError:
        print("CSV file not found.")
        return

    html_content.append("""
            </tbody>
        </table>
    </body>
    </html>
    """)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("".join(html_content))

    print(f"HTML report generated: {output_file}")

if __name__ == "__main__":
    main()
