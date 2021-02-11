

Several important pitfalls:
- `bq_out` specifies plugin to use (`bq`) and output directory for plugin
- `bq_opt` specifies additional options for plugin
- `proto-gen-<name>` prefix determines plugin name
- `--plugin=<name>=<file>` 


Template idea for multi-language support:
```python
class LanguageConverter:

    def generate_class(self):
        pass
        
    def generate_function(self):
        pass
    
    def generate_if_else(self):
        pass
    
    def generate_loop(self):
        pass
    
    # TODO: maybe not needed?
    def generate_exception(self):
        pass
    
    def generate_get_variable(self, v: Variable):
        pass
    
    def generate_has_variable(self, v: Variable):
        pass
    
    def generate_set_variable(self, v: Variable):
        pass
    
    def generate_initialize_variable(self, v: Variable):
        pass

   

```