import random
from benchmarks.core.base_test import BasePerformanceTest


class AddNutritionForLastRecipeTest(BasePerformanceTest):
    def __init__(self):
        super().__init__(
            description="INSERT nutrition dla ostatniego przepisu (MAX(id))",
            operation="CREATE")

    _sql = """
        INSERT INTO nutrition (id_recipe,calories,carbohydrates,protein,fat,fiber,salt,saturated_fat,sugars)
        SELECT MAX(id), 500,50,30,20,5,0.5,10,15 FROM recipes;
    """

    def test_mysql(self):
        cur = self._mysql.cursor()
        cur.execute(self._sql)
        cur.close()
        return 1

    def test_postgres(self):
        cur = self._postgres.cursor()
        cur.execute(self._sql)
        cur.close()
        return 1

    def test_mongo(self, db):
        rid = db.recipes.find_one(sort=[("id", -1)])["id"]
        doc = {
            "id": db.nutrition.estimated_document_count()+1,
            "id_recipe": rid,
            "calories": 500,
            "carbohydrates": 50,
            "protein": 30,
            "fat": 20,
            "fiber": 5,
            "salt": 0.5,
            "saturated_fat": 10,
            "sugars": 15
        }
        return db.nutrition.insert_one(doc).inserted_id
