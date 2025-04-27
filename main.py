import inspect
import threading
import tkinter as tk
from tkinter import ttk, messagebox

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

from benchmarks.core.base_test import BasePerformanceTest
from benchmarks.tests import (
    test_01_basic_read,
    test_02_rating_by_cuisine,
    test_03_top_healthy_popular_recipes,
    test_04_highly_rated_unliked_recipes,
)
from database_scripts.record_counter import get_record_counts, human_readable
from generate_and_import import generate_and_import


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Benchmark GUI")
        self.geometry("600x320")
        self.resizable(False, False)

        # wczytaj klasy testów i pogrupuj je po operacjach CRUD
        self.tests_by_op = self._load_test_classes()

        self._create_tabs()

    # ---------- zakładki --------------------------------------------------

    def _create_tabs(self):
        tab_control = ttk.Notebook(self)
        tab_control.pack(expand=1, fill="both")

        self.tab_tests = ttk.Frame(tab_control)
        self.tab_generate = ttk.Frame(tab_control)
        tab_control.add(self.tab_tests, text="🧪 Testy")
        tab_control.add(self.tab_generate, text="⚙️ Generowanie danych")

        self._init_tab_tests()
        self._init_tab_generate()

    # ---------- zakładka TESTY -------------------------------------------

    def _init_tab_tests(self):
        frame = ttk.Frame(self.tab_tests)
        frame.pack(pady=10, padx=10, fill="both", expand=True)

        # ---------- wybór operacji CRUD ----------
        ttk.Label(frame, text="Operacja CRUD:", font=("Arial", 12)).grid(
            row=0, column=0, sticky="w"
        )
        self.op_selector = ttk.Combobox(frame, state="readonly", width=10)
        self.op_selector["values"] = ["CREATE", "READ", "UPDATE", "DELETE"]
        self.op_selector.current(1)  # READ domyślnie
        self.op_selector.grid(row=0, column=1, sticky="w", padx=(5, 20))
        self.op_selector.bind("<<ComboboxSelected>>", self._on_operation_selected)

        # ---------- wybór testu ----------
        ttk.Label(frame, text="Test:", font=("Arial", 12)).grid(
            row=0, column=2, sticky="w"
        )
        self.test_selector = ttk.Combobox(frame, state="readonly", width=35)
        self.test_selector.grid(row=0, column=3, sticky="w")
        self.test_selector.bind("<<ComboboxSelected>>", self._on_test_selected)

        # ---------- opis testu ----------
        self.test_description = tk.Text(
            frame, height=4, wrap="word", width=70, state="disabled"
        )
        self.test_description.grid(row=1, column=0, columnspan=4, pady=(10, 10))

        # wypełnij testy dla domyślnej operacji
        self._populate_tests_for_operation("READ")

        # ---------- silniki baz ----------
        ttk.Label(frame, text="Silniki baz danych:").grid(
            row=2, column=0, columnspan=4, sticky="w", pady=(10, 0)
        )

        self.selected_engines = {
            "mysql": tk.BooleanVar(value=True),
            "postgres": tk.BooleanVar(value=True),
            "mongo_latest": tk.BooleanVar(value=True),
            "mongo_old": tk.BooleanVar(value=True),
        }
        for idx, (key, var) in enumerate(self.selected_engines.items()):
            ttk.Checkbutton(frame, text=key, variable=var).grid(
                row=3, column=idx, sticky="w"
            )

        # ---------- przycisk RUN + status ----------
        self.run_button = ttk.Button(
            frame, text="▶ Uruchom test", command=self._run_selected_test
        )
        self.run_button.grid(row=4, column=0, pady=20, sticky="w")

        self.test_status_label = ttk.Label(frame, text="", font=("Arial", 10, "italic"))
        self.test_status_label.grid(row=4, column=1, columnspan=3, sticky="w")

        # ---------- liczba rekordów ----------
        self.mysql_label = ttk.Label(
            frame, text="📊 MySQL: ... rekordów", font=("Arial", 10)
        )
        self.mysql_label.grid(row=5, column=0, columnspan=4, sticky="w")

        # inicjalne odczytanie liczby rekordów
        self._update_mysql_count()

        # odświeżaj liczbę rekordów, gdy wracamy na zakładkę
        self.tab_tests.bind("<Visibility>", lambda e: self._update_mysql_count())

    # ─────────────────────────────────────────────────────────────────────────────
    #   ZAKŁADKA 2 – Generowanie danych
    # ─────────────────────────────────────────────────────────────────────────────
    def _init_tab_generate(self):
        frame = ttk.Frame(self.tab_generate)
        frame.pack(pady=10, padx=10, fill="both", expand=True)

        # ── wybór wielkości
        ttk.Label(frame, text="Liczba rekordów:", font=("Arial", 12)).grid(row=0, column=0, sticky="w")
        self.size_options = {"1k": 1_000, "10k": 10_000, "100k": 100_000, "1M": 1_000_000, "10M": 10_000_000}
        self.size_selector = ttk.Combobox(frame, state="readonly", values=list(self.size_options.keys()), width=10)
        self.size_selector.current(2)  # 100k
        self.size_selector.grid(row=0, column=1, sticky="w", padx=(5, 20))

        # ── przycisk
        self.generate_btn = ttk.Button(frame, text="⚙️  Generuj dane", command=self._start_generation)
        self.generate_btn.grid(row=1, column=0, sticky="w", pady=10)

        # ── status + progress
        self.gen_status = ttk.Label(frame, text="🟢 Gotowy", font=("Arial", 10, "italic"))
        self.gen_status.grid(row=1, column=1, sticky="w", padx=10)

        self.progress = ttk.Progressbar(frame, orient="horizontal", mode="indeterminate", length=400)
        self.progress.grid(row=2, column=0, columnspan=3, pady=10)

        # ── pole podsumowania
        self.summary_box = tk.Text(frame, height=10, width=70, state="disabled")
        self.summary_box.grid(row=3, column=0, columnspan=3, pady=5)

    # ─────────────────────────────────────────────────────────────────────────────
    def _start_generation(self):
        label = self.size_selector.get()
        if not label:
            messagebox.showwarning("Błąd", "Wybierz liczbę rekordów.")
            return

        total = self.size_options[label]
        self.gen_status.config(text="⏳ Generowanie…")
        self.generate_btn.config(state="disabled")
        self.progress.start(10)

        def task():
            try:
                summary = generate_and_import(total)

                def on_done():
                    self.progress.stop()
                    self.gen_status.config(text="✅ Zakończono")
                    self.generate_btn.config(state="normal")
                    self._update_mysql_count()  # odśwież licznik w zakładce testów

                    self.summary_box.configure(state="normal")
                    self.summary_box.delete("1.0", tk.END)
                    for k, v in summary.items():
                        self.summary_box.insert(tk.END, f"{k}: {v:,}\n")
                    self.summary_box.configure(state="disabled")

                self.after(0, on_done)
            except Exception as e:
                raise e
                self.after(0, lambda: self._generation_error(e))

        threading.Thread(target=task, daemon=True).start()

    def _generation_error(self, err):
        self.progress.stop()
        self.gen_status.config(text=f"❌ Błąd: {err}")
        self.generate_btn.config(state="normal")

    # ----------  helpery GUI  --------------------------------------------

    def _populate_tests_for_operation(self, operation):
        """Ustaw listę w comboboxie na testy z danej kategorii."""
        tests = self.tests_by_op.get(operation.upper(), [])
        self.test_selector["values"] = [cls.__name__ for cls in tests]
        if tests:
            self.test_selector.current(0)
            self._on_test_selected(None)
        else:
            self.test_selector.set("")
            self.test_description.configure(state="normal")
            self.test_description.delete("1.0", tk.END)
            self.test_description.insert(
                tk.END, "Brak testów dla wybranej operacji."
            )
            self.test_description.configure(state="disabled")

    # ---------- zdarzenia  ------------------------------------------------

    def _on_operation_selected(self, _event):
        op = self.op_selector.get()
        self._populate_tests_for_operation(op)

    def _on_test_selected(self, _event):
        class_name = self.test_selector.get()
        if not class_name:
            return
        cls = self._find_test_class_by_name(class_name)
        description = getattr(cls(), "description") or "Brak opisu"
        print(description)
        self.test_description.configure(state="normal")
        self.test_description.delete("1.0", tk.END)
        self.test_description.insert(tk.END, description)
        self.test_description.configure(state="disabled")

    # ---------- logika TESTÓW  ------------------------------------------

    def _run_selected_test(self):
        class_name = self.test_selector.get()
        if not class_name:
            messagebox.showwarning("Błąd", "Wybierz test.")
            return

        cls = self._find_test_class_by_name(class_name)
        if cls is None:
            messagebox.showerror("Błąd", "Nie znaleziono klasy testu.")
            return

        selected_engines = [k for k, v in self.selected_engines.items() if v.get()]
        if not selected_engines:
            messagebox.showwarning("Błąd", "Wybierz przynajmniej jeden silnik.")
            return

        self.test_status_label.config(text="⏳ Trwa wykonywanie testu...")

        def task():
            try:
                runner = cls()
                results = runner.run()
                filtered = {k: v for k, v in results.items() if k in selected_engines}
                self.after(
                    0, lambda: self._show_results_chart(class_name, filtered)
                )
                self.after(
                    0, lambda: self.test_status_label.config(text="✅ Test zakończony")
                )
                self._update_mysql_count()
            except Exception as e:
                self.after(
                    0,
                    lambda: self.test_status_label.config(text=f"❌ Błąd: {e}"),
                )

        threading.Thread(target=task, daemon=True).start()

    # ---------- wykres ----------------------------------------------------

    def _show_results_chart(self, title, results):
        top = tk.Toplevel(self)
        top.title(f"Wyniki testu: {title}")
        top.geometry("640x420")

        engines = list(results.keys())
        values = [results[e] if isinstance(results[e], (int, float)) else 0 for e in engines]

        fig = Figure(figsize=(6, 4), dpi=100)
        ax = fig.add_subplot(111)
        ax.bar(engines, values)
        ax.set_title(title)
        ax.set_ylabel("Czas [s]")
        ax.set_xlabel("Baza danych")

        canvas = FigureCanvasTkAgg(fig, master=top)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    # ---------- liczba rekordów ------------------------------------------

    def _update_mysql_count(self):
        def task():
            counts = get_record_counts()
            mysql_total = counts.get("MySQL", "błąd")
            pretty = human_readable(mysql_total) if isinstance(mysql_total, int) else mysql_total
            self.after(0, lambda: self.mysql_label.config(text=f"📊 MySQL: {pretty}"))

        threading.Thread(target=task, daemon=True).start()

    # ---------- ładowanie klas  ------------------------------------------

    def _load_test_classes(self):
        modules = [
            test_01_basic_read,
            test_02_rating_by_cuisine,
            test_03_top_healthy_popular_recipes,
            test_04_highly_rated_unliked_recipes,
        ]
        by_op = {"CREATE": [], "READ": [], "UPDATE": [], "DELETE": []}

        for module in modules:
            for _name, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, BasePerformanceTest) and obj is not BasePerformanceTest:
                    op = getattr(obj(), "operation", "READ").upper()
                    if op not in by_op:
                        op = "READ"
                    by_op[op].append(obj)

        return by_op

    def _find_test_class_by_name(self, name: str):
        for lst in self.tests_by_op.values():
            for cls in lst:
                if cls.__name__ == name:
                    return cls
        return None


if __name__ == "__main__":
    App().mainloop()
