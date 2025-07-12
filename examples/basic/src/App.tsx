import { useDB, useQuery } from "@jakobsaadbye/teilen-sql/react";

export type Recipe = {
  id: string
  title: string
}


const randomRecipeTitles = [
  "Spaghetti Carbonara",
  "Thai Green Curry",
  "Chicken Alfredo",
  "Vegetarian Chili",
  "Beef Stroganoff",
  "Lemon Garlic Salmon",
  "Butternut Squash Soup",
  "BBQ Pulled Pork Sandwich",
  "Shrimp Tacos with Mango Salsa",
  "Pesto Pasta Salad",
  "Eggplant Parmesan",
  "Mushroom Risotto",
  "Teriyaki Chicken Bowl",
  "Moroccan Chickpea Stew",
  "Buffalo Cauliflower Bites",
  "Classic Beef Burgers",
  "Vegan Buddha Bowl",
  "Avocado Toast with Poached Egg",
  "Garlic Butter Steak Bites",
  "Zucchini Noodles with Pesto"
];

const getRandomRecipeTitle = () => {
  const r = Math.round(Math.random() * (randomRecipeTitles.length - 1));
  return randomRecipeTitles[r];
}


export const App = () => {

  const db = useDB();

  const recipeTable = db.tables[0];

  const recipes = useQuery<Recipe[]>(`SELECT * FROM "recipes"`, []).data;

  const insertRandomRecipe = () => {
    const r: Recipe = {
      id: crypto.randomUUID(),
      title: getRandomRecipeTitle()
    }

    saveRecipe(r);
  }

  const saveRecipe = async (r: Recipe) => {
    await db.exec(`
      INSERT INTO "recipes" (id, title)
      VALUES (?, ?)
    `, [r.id, r.title]);
  }

  return (
    <main className='bg-nice-gray min-h-screen min-w-screen text-white'>
      <h1 className='p-8 text-5xl font-semibold text-center'>Teilen Auto-migrator</h1>


      <div className="flex flex-col items-center gap-y-8">
        <button className="px-8 py-2 bg-blue-500 rounded-sm" onClick={insertRandomRecipe}>+ Insert random</button>

        <table className="min-w-[300px] border-1 border-white">
          <thead className="">
            <tr className="">
              {recipeTable.columns.map((c, i) => {
                return (
                  <th key={i} className="p-2 border-r-1 border-white">{c.name}</th>
                )
              })}
            </tr>
          </thead>

          <tbody className="px-2">
            {recipes && recipes.map((r, i) => {
              return (
                <tr key={i}>
                  {Object.values(r).map((v, i) => <td className="p-2 border-r-1 border-white">{v}</td>)}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </main>
  )
}