import csv, os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

def import_csv(file_name):
    temp = []
    with open(os.path.join(__location__, file_name)) as f:
        rows = csv.DictReader(f)
        for r in rows:
            temp.append(dict(r))
    return temp


cities = import_csv("Cities.csv")

countries = import_csv('Countries.csv')

titanic = import_csv('Titanic.csv')

players = import_csv('Players.csv')

teams = import_csv('Teams.csv')


class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None
    
import copy
class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table
    
    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + '_joins_' + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)

        return joined_table
    
    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table
    
    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            temps.append(float(item1[aggregation_key]))
        return function(temps)
    
    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def __str__(self):
        return self.table_name + ':' + str(self.table)


table1 = Table('cities', cities)
table2 = Table('countries', countries)
table3 = Table('players', players)
table4 = Table('teams', teams)
table5 = Table('titanic', titanic)

my_DB = DB()

my_DB.insert(table1)
my_DB.insert(table2)
my_DB.insert(table3)
my_DB.insert(table4)
my_DB.insert(table5)

print("\n====================================================================\n")

# What player on a team with “ia” in the team name played less than 200 minutes and made more than 100 passes?
# Select to display the player

selected_player = table3.filter(lambda player : "ia" in player["team"] and float(player["minutes"]) < 200 and int(player["passes"]) > 100)

print("Player in a team that have ia in team name that play less then 200 \nminutes and passes more then 100 times\n")

for player in selected_player.table:
    print(player["surname"], player["team"], player["position"], sep="\n")

print("\n====================================================================\n")

# The average number of games played for teams ranking below 10 versus teams ranking     above or equal 10

more_then_10_team = table4.filter(lambda team : int(team['ranking']) >= 10).aggregate(lambda team_game : sum(team_game) / len(team_game), 'games')
less_then_10_team = table4.filter(lambda team : int(team['ranking']) < 10).aggregate(lambda team_game : sum(team_game) / len(team_game), 'games')

print("The average number of games played for teams ranking below 10")
print(less_then_10_team)

print("The average number of games played for teams ranking above and equal to 10")
print(more_then_10_team)

print("\n====================================================================\n")

# The average number of passes made by forwards versus by midfielders

forward = table3.filter(lambda player: player["position"] == "forward").aggregate(lambda pass_count: sum(pass_count) / len(pass_count), 'passes')
midfielder = table3.filter(lambda player: player["position"] == "midfielder").aggregate(lambda pass_count: sum(pass_count) / len(pass_count), 'passes')

print("The average number of passes made by forwards")
print(forward)

print("The average number of passes made by midfielder")
print(midfielder)

print("\n====================================================================\n")

# The average fare paid by passengers in the first class versus in the third class

first_class = table5.filter(lambda passenger: passenger['class'] == '1').aggregate(lambda fare: sum(fare) / len(fare), "fare")
third_class = table5.filter(lambda passenger: passenger['class'] == '3').aggregate(lambda fare: sum(fare) / len(fare), "fare")
print("The average fare paid by passengers in the first class")
print(first_class)
print("The average fare paid by passengers in the third class")
print(third_class)

print("\n====================================================================\n")

male = table5.filter(lambda passenger: passenger['gender'] == 'M')
male_survive = table5.filter(lambda passenger: passenger['gender'] == 'M').filter(lambda male: male['survived'] == "yes")
male_survive_rate = (len(male_survive.table) / len(male.table)) * 100

female = table5.filter(lambda passenger: passenger['gender'] == 'F')
female_survive = table5.filter(lambda passenger: passenger['gender'] == 'F').filter(lambda female: female['survived'] == "yes")
female_survive_rate = (len(female_survive.table) / len(female.table)) * 100

print("Male survive rate")
print(male_survive_rate)
print("female survive rate")
print(female_survive_rate)

# my_table1 = my_DB.search('cities')
#
# print("Test filter: only filtering out cities in Italy")
# my_table1_filtered = my_table1.filter(lambda x: x['country'] == 'Italy')
# print(my_table1_filtered)
# print()
#
# print("Test select: only displaying two fields, city and latitude, for cities in Italy")
# my_table1_selected = my_table1_filtered.select(['city', 'latitude'])
# print(my_table1_selected)
# print()
#
# print("Calculting the average temperature without using aggregate for cities in Italy")
# temps = []
# for item in my_table1_filtered.table:
#     temps.append(float(item['temperature']))
# print(sum(temps)/len(temps))
# print()
#
# print("Calculting the average temperature using aggregate for cities in Italy")
# print(my_table1_filtered.aggregate(lambda x: sum(x)/len(x), 'temperature'))
# print()
#
# print("Test join: finding cities in non-EU countries whose temperatures are below 5.0")
# my_table2 = my_DB.search('countries')
# my_table3 = my_table1.join(my_table2, 'country')
# my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'no').filter(lambda x: float(x['temperature']) < 5.0)
# print(my_table3_filtered.table)
# print()
# print("Selecting just three fields, city, country, and temperature")
# print(my_table3_filtered.select(['city', 'country', 'temperature']))
# print()
#
# print("Print the min and max temperatures for cities in EU that do not have coastlines")
# my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'yes').filter(lambda x: x['coastline'] == 'no')
# print("Min temp:", my_table3_filtered.aggregate(lambda x: min(x), 'temperature'))
# print("Max temp:", my_table3_filtered.aggregate(lambda x: max(x), 'temperature'))
# print()
#
# print("Print the min and max latitude for cities in every country")
# for item in my_table2.table:
#     my_table1_filtered = my_table1.filter(lambda x: x['country'] == item['country'])
#     if len(my_table1_filtered.table) >= 1:
#         print(item['country'], my_table1_filtered.aggregate(lambda x: min(x), 'latitude'), my_table1_filtered.aggregate(lambda x: max(x), 'latitude'))
# print()
