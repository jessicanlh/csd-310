import tkinter as tk

class Todo(tk.Tk):
    def __init__(self, tasks=None):
        super().__init__()

        if not tasks:
            self.tasks = []
        else:
            self.tasks = tasks

        self.title("Long-Heinicke-ToDo") #Change widget name
        self.geometry("300x400")

        #Change header
        todo1 = tk.Label(self, text="--- Add Items Here --- Right Click to Delete ---", bg="blue", fg="white", pady=10)

        self.tasks.append(todo1)

        for task in self.tasks:
            task.pack(side=tk.TOP, fill=tk.X)
            task.bind("<Button-3>", self.delete_task)  # Bind right-click for deletion

        self.task_create = tk.Text(self, height=3, bg="white", fg="black")

        self.task_create.pack(side=tk.BOTTOM, fill=tk.X)
        self.task_create.focus_set()

        self.bind("<Return>", self.add_task)

        #Change color scheme
        self.colour_schemes = [{"bg": "blue", "fg": "white"}, {"bg": "orange", "fg": "black"}]

    def add_task(self, event=None):
        task_text = self.task_create.get(1.0, tk.END).strip()

        if len(task_text) > 0:
            new_task = tk.Label(self, text=task_text, pady=10)

            _, task_style_choice = divmod(len(self.tasks), 2)

            my_scheme_choice = self.colour_schemes[task_style_choice]

            new_task.configure(bg=my_scheme_choice["bg"])
            new_task.configure(fg=my_scheme_choice["fg"])

            new_task.pack(side=tk.TOP, fill=tk.X)
            new_task.bind("<Button-3>", self.delete_task)  # Bind right-click for deletion

            self.tasks.append(new_task)

        self.task_create.delete(1.0, tk.END)

    def delete_task(self, event):
        task_to_remove = event.widget  # Get the widget that triggered the event
        self.tasks.remove(task_to_remove)  # Remove from the tasks list
        task_to_remove.destroy()  # Remove the widget from the GUI

    #create menu bar with file -> exit option
    def create_menu(self):
        menu_bar = Menu(self)
        self.config(menu=menu_bar)

        file_menu = Menu(menu_bar, tearoff=0)
        file_menu.add_command(label="Exit", command=self.exit_program)

        menu_bar.add_cascade(label="File", menu=file_menu)

    def exit_program(self):
        self.destroy()

if __name__ == "__main__":
    todo = Todo()
    todo.mainloop()
