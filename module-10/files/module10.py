import tkinter as tk
from tkinter import Menu

class Todo(tk.Tk):
    def __init__(self, tasks=None):
        super().__init__()

        if not tasks:
            self.tasks = []
        else:
            self.tasks = tasks

        self.title("Long-Heinicke-ToDo")
        self.geometry("300x400")

        # Add the menu bar
        self.create_menu()

        # Create a default task
        todo1 = tk.Label(self, text="--- Add Items Here --- Right Click to Delete ---", bg="blue", fg="white", pady=10)
        self.tasks.append(todo1)

        # Display the default task and bind right-click
        for task in self.tasks:
            task.pack(side=tk.TOP, fill=tk.X)
            task.bind("<Button-2>" if self.is_mac() else "<Button-3>", self.delete_task)

        self.task_create = tk.Text(self, height=3, bg="white", fg="black")
        self.task_create.pack(side=tk.BOTTOM, fill=tk.X)
        self.task_create.focus_set()

        self.bind("<Return>", self.add_task)

        self.colour_schemes = [{"bg": "blue", "fg": "white"}, {"bg": "orange", "fg": "black"}]

    # Check if os is mac because nothing is simple with a mac
    def is_mac(self):
        import platform
        return platform.system() == "Darwin"

    # Create in window menu bar
    def create_menu(self):
        menu_frame = tk.Frame(self, bg="lightgray", height=30)
        menu_frame.pack(side=tk.TOP, fill=tk.X)

        file_button = tk.Menubutton(menu_frame, text="File", bg="black", relief=tk.RAISED)
        file_button.pack(side=tk.LEFT, padx=5)

        file_menu = tk.Menu(file_button, tearoff=0)
        file_button["menu"] = file_menu

        file_menu.add_command(label="Exit", command=self.exit_program)

    def add_task(self, event=None):
        task_text = self.task_create.get(1.0, tk.END).strip()

        if len(task_text) > 0:
            new_task = tk.Label(self, text=task_text, pady=10)

            # Apply alternating color schemes
            _, task_style_choice = divmod(len(self.tasks), 2)
            my_scheme_choice = self.colour_schemes[task_style_choice]
            new_task.configure(bg=my_scheme_choice["bg"], fg=my_scheme_choice["fg"])

            # Display and bind right-click
            new_task.pack(side=tk.TOP, fill=tk.X)
            new_task.bind("<Button-2>" if self.is_mac() else "<Button-3>", self.delete_task)

            self.tasks.append(new_task)

        self.task_create.delete(1.0, tk.END)

    # Delete Tasks
    def delete_task(self, event):
        task_to_remove = event.widget  # Get the widget that triggered the event
        if task_to_remove in self.tasks:
            self.tasks.remove(task_to_remove)  # Remove from the tasks list
            task_to_remove.destroy()  # Destroy the widget

    # Exit the application
    def exit_program(self):
        self.destroy()

if __name__ == "__main__":
    todo = Todo()
    todo.mainloop()
