using System;
using Terminal.Gui;

namespace Storage.Net.Cli
{
   class Program
   {
      static void Main(string[] args)
      {
         Application.Init();

         Toplevel top = Application.Top;
         var win = new Window(new Rect(0, 1, top.Frame.Width, top.Frame.Height - 1), "Storage.Net CLI");
         top.Add(win);

         // Creates a menubar, the item "New" has a help menu.
         var menu = new MenuBar(new MenuBarItem[] {
            new MenuBarItem ("_Storage", new MenuItem [] {
                new MenuItem ("_New", "Creates new file", SampleAction),
                new MenuItem ("_Close", "", () => Close ()),
                new MenuItem ("_Quit", "", () => { if (Quit ()) top.Running = false; })
            }),
            new MenuBarItem ("_Edit", new MenuItem [] {
                new MenuItem ("_Copy", "", null),
                new MenuItem ("C_ut", "", null),
                new MenuItem ("_Paste", "", null)
            })
         });

         top.Add(menu);

         // Add some controls
         win.Add(
                 new Label(3, 2, "Login: "),
                 new TextField(14, 2, 40, ""),
                 new Label(3, 4, "Password: "),
                 new TextField(14, 4, 40, "") { Secret = true },
                 new CheckBox(3, 6, "Remember me"),
                 new RadioGroup(3, 8, new[] { "_Personal", "_Company" }),
                 new Button(3, 14, "Ok"),
                 new Button(10, 14, "Cancel"),
                 new Label(3, 18, "Press ESC and 9 to activate the menubar"));

         Application.Run();
      }


      static bool Quit()
      {
         int n = MessageBox.Query(50, 7, "Quit Demo", "Are you sure you want to quit this demo?", "Yes", "No");

         return n == 0;
      }



      static void Close()
      {
         MessageBox.ErrorQuery(50, 5, "Error", "There is nothing to close", "Ok");
      }

      static void SampleAction()
      {

      }
   }
}
