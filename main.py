import inspect
import threading
import tkinter as tk
from tkinter import ttk, messagebox

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

from benchmarks.core.base_test import BasePerformanceTest
from benchmarks.tests import test_01_basic_read, test_02_rating_by_cuisine, test_03_top_healthy_popular_recipes, \
    test_04_highly_rated_unliked_recipes
from database_scripts.record_counter import get_record_counts


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Benchmark GUI")
        self.geometry("510x400")
        self.resizable(False, False)
        self.test_classes = self._load_test_classes()
        self._create_tabs()

    def _create_tabs(self):
        tab_control = ttk.Notebook(self)
        tab_control.pack(expand=1, fill='both')

        self.tab_tests = ttk.Frame(tab_control)
        self.tab_generate = ttk.Frame(tab_control)
        self.tab_status = ttk.Frame(tab_control)

        tab_control.add(self.tab_tests, text="🧪 Testy")
        tab_control.add(self.tab_generate, text="⚙️ Generowanie danych")

        self._init_tab_tests()
        self._init_tab_generate()

    def _init_tab_tests(self):
        self.test_tab_frame = ttk.Frame(self.tab_tests)
        self.test_tab_frame.pack(pady=10, padx=10, fill='both', expand=True)

        ttk.Label(self.test_tab_frame, text="Wybierz test:", font=("Arial", 12)).grid(row=0, column=0, columnspan=2,
                                                                                      sticky="w")

        self.test_selector = ttk.Combobox(self.test_tab_frame, state="readonly")
        self.test_selector["values"] = list(self.test_classes.keys())
        self.test_selector.grid(row=0, column=2, columnspan=2, sticky="we")
        self.test_selector.bind("<<ComboboxSelected>>", self._on_test_selected)

        self.test_description = tk.Text(self.test_tab_frame, height=4, wrap="word", width=60, state='disabled')
        self.test_description.grid(row=1, column=0, columnspan=4, pady=(10, 10))

        ttk.Label(self.test_tab_frame, text="Wybierz silniki baz danych:").grid(row=2, column=0, columnspan=4,
                                                                                sticky="w", pady=(10, 0))

        self.selected_engines = {
            "mysql": tk.BooleanVar(value=True),
            "postgres": tk.BooleanVar(value=True),
            "mongo_latest": tk.BooleanVar(value=True),
            "mongo_old": tk.BooleanVar(value=True),
        }

        for idx, (key, var) in enumerate(self.selected_engines.items()):
            ttk.Checkbutton(self.test_tab_frame, text=key, variable=var).grid(row=3, column=0 + idx, sticky="w")

        self.run_button = ttk.Button(self.test_tab_frame, text="▶ Uruchom test", command=self._run_selected_test)
        self.run_button.grid(row=4, column=0, columnspan=1, pady=20, sticky="w")

        self.test_status_label = ttk.Label(self.test_tab_frame, text="", font=("Arial", 10, "italic"))
        self.test_status_label.grid(row=4, column=1, columnspan=3, sticky="w")

        self.status_label = ttk.Label(self.test_tab_frame, text="📊 Liczba rekordów...", font=("Arial", 10))
        self.status_label.grid(row=5, column=0, columnspan=4, sticky="w")

        self._load_db_record_counts()  # Początkowy odczyt

        self.tab_tests.bind("<Visibility>", lambda e: self._load_db_record_counts())

    def _init_tab_generate(self):
        frame = ttk.Frame(self.tab_generate)
        frame.pack(pady=10, padx=10, fill='both', expand=True)

        ttk.Label(frame, text="Wybierz liczbę rekordów:", font=("Arial", 12)).grid(row=0, column=0, sticky="w")

        self.size_options = {
            "1k": 1_000,
            "10k": 10_000,
            "100k": 100_000,
            "1M": 1_000_000,
            "10M": 10_000_000
        }

        self.size_selector = ttk.Combobox(frame, state="readonly")
        self.size_selector["values"] = list(self.size_options.keys())
        self.size_selector.current(2)  # domyślnie 100k
        self.size_selector.grid(row=0, column=1, sticky="w")

        self.generate_button = ttk.Button(frame, text="⚙️ Generuj dane", command=self._start_data_generation)
        self.generate_button.grid(row=1, column=0, columnspan=2, pady=10, sticky="w")

        self.gen_status_label = ttk.Label(frame, text="🟢 Gotowy", font=("Arial", 10, "italic"))
        self.gen_status_label.grid(row=1, column=2, sticky="w", padx=10)

        self.progress = ttk.Progressbar(frame, orient="horizontal", mode="indeterminate", length=400)
        self.progress.grid(row=2, column=0, columnspan=3, pady=10)

        self.summary_text = tk.Text(frame, height=10, width=70)
        self.summary_text.grid(row=3, column=0, columnspan=4, pady=10)


    def _load_test_classes(self):
        modules = [
            test_01_basic_read,
            test_02_rating_by_cuisine,
            test_03_top_healthy_popular_recipes,
            test_04_highly_rated_unliked_recipes
        ]

        tests = {}
        for module in modules:
            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and issubclass(obj, BasePerformanceTest) and obj is not BasePerformanceTest:
                    tests[name] = obj
        return tests

    def _on_test_selected(self, event):
        class_name = self.test_selector.get()
        cls = self.test_classes[class_name]
        description = cls().description or 'Brak opisu'
        self.test_description.configure(state='normal')
        self.test_description.delete("1.0", tk.END)
        self.test_description.insert(tk.END, description)
        self.test_description.configure(state='disabled')

    def _run_selected_test(self):
        class_name = self.test_selector.get()
        if not class_name:
            messagebox.showwarning("Błąd", "Wybierz test.")
            return

        cls = self.test_classes[class_name]()
        selected = [k for k, v in self.selected_engines.items() if v.get()]
        if not selected:
            messagebox.showwarning("Błąd", "Wybierz przynajmniej jeden silnik.")
            return

        self.test_status_label.config(text="⏳ Trwa wykonywanie testu...")

        def task():
            try:
                results = cls.run()
                filtered_results = {k: v for k, v in results.items() if k in selected}
                self.after(0, lambda: self._show_results_chart(class_name, filtered_results))
                self.after(0, lambda: self.test_status_label.config(text="✅ Test zakończony."))
            except Exception as e:
                self.after(0, lambda: self.test_status_label.config(text=f"❌ Błąd: {e}"))

        threading.Thread(target=task, daemon=True).start()

    def _show_results_chart(self, title, results):
        top = tk.Toplevel(self)
        top.title(f"Wyniki testu: {title}")
        top.geometry("600x400")

        engines = list(results.keys())
        values = [results[k] if isinstance(results[k], (int, float)) else 0 for k in engines]

        fig = Figure(figsize=(6, 4), dpi=100)
        ax = fig.add_subplot(111)
        ax.bar(engines, values)
        ax.set_title(f"Wyniki testu: {title}")
        ax.set_ylabel("Czas [s]")
        ax.set_xlabel("Baza danych")

        canvas = FigureCanvasTkAgg(fig, master=top)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    def _load_db_record_counts(self):
        def task():
            counts = get_record_counts()
            text = "📊 Rekordy w bazach danych:\n"
            for db, count in counts.items():
                text += f" - {db}: {count}\n"

            def update_label():
                self.status_label.config(text=f'Liczba rekordów: {self._human_readable_count(counts['MySQL'])}')

            self.after(0, update_label)

        threading.Thread(target=task, daemon=True).start()

    def _start_data_generation(self):
        label = self.size_selector.get()
        if not label:
            messagebox.showwarning("Błąd", "Wybierz liczbę rekordów.")
            return

        total_records = self.size_options[label]

        self.gen_status_label.config(text="⏳ Generowanie danych...")
        self.progress.start(10)
        self.generate_button.config(state="disabled")

        def task():
            try:
                from scripts.data_generator import generate_data_and_import  # zakładamy taką funkcję

                summary = generate_data_and_import(total_records)

                def on_done():
                    self.progress.stop()
                    self.generate_button.config(state="normal")
                    self.gen_status_label.config(text="✅ Zakończono")
                    self._load_db_record_counts()

                    self.summary_text.delete("1.0", tk.END)
                    for k, v in summary.items():
                        self.summary_text.insert(tk.END, f"{k}: {v:,}\n")

                self.after(0, on_done)
            except Exception as e:
                def on_error():
                    self.progress.stop()
                    self.generate_button.config(state="normal")
                    self.gen_status_label.config(text=f"❌ Błąd: {e}")
                self.after(0, on_error)

        threading.Thread(target=task, daemon=True).start()


    def _human_readable_count(self, n):
        if n >= 1_000_000:
            return f"{round(n / 1_000_000, 1)} mln"
        elif n >= 1_000:
            return f"{round(n / 1_000, 1)} tys."
        return str(n)


if __name__ == "__main__":
    app = App()
    app.mainloop()
