install.packages("basedosdados")
library("basedosdados")
library("dplyr")
library("tidyverse")
library("writexl")
library("janitor")
library("rJava")
system("java -version")

# Defina o seu projeto no Google Cloud
set_billing_id("basedosdados-379114")

# Visualização do dicionário da base
query <- bdplyr("br_me_rais.dicionario")
dic <- bd_collect(query)
View(dic)


# Para carregar o dado direto no R filtrando por ano 2021 e município de São Paulo
query <- bdplyr("br_me_rais.microdados_vinculos") %>% 
  filter(ano == 2021,
         id_municipio == "3550308") %>% 
  select(
    "vinculo_ativo_3112",
    "tipo_vinculo",
    "tipo_admissao",
    "mes_admissao",
    "mes_desligamento",
    "motivo_desligamento",
    "faixa_remuneracao_media_sm",
    "valor_remuneracao_media",
    "faixa_remuneracao_dezembro_sm",
    "valor_remuneracao_dezembro_sm",
    "cnae_2",
    "cnae_2_subclasse",
    "idade",
    "faixa_etaria",
    "sexo",
    "raca_cor",
    "distritos_sp",
    "tipo_estabelecimento",
    "tamanho_estabelecimento",
    "grau_instrucao_apos_2005",
    )
  
tabela <- bd_collect(query)

# Importando dataset de População em Idade Ativa 



# Criando as medidas de total de vínculos ativos em 31/12/2020 e média de remuneração por distrito

df <- tabela %>% 
  mutate(distritos_sp = recode(distritos_sp, 
                               "0001" = "Água Rasa",
                               "0002" = "Alto De Pinheiros",
                               "0003" = "Anhanguera",
                               "0004" = "Aricanduva",
                               "0005" = "Artur Alvim",
                               "0006" = "Barra Funda",
                               "0007" = "Bela Vista",
                               "0008" = "Belém",
                               "0009" = "Bom Retiro",
                               "0010" = "Brás",
                               "0011" = "Brasilândia",
                               "0012" = "Butantã",
                               "0013" = "Cambuci",
                               "0014" = "Campo Belo",
                               "0015" = "Campo Grande",
                               "0016" = "Campo Limpo",
                               "0017" = "Cangaiba",
                               "0018" = "Capão Redondo",
                               "0019" = "Carrão",
                               "0020" = "Casa Verde",
                               "0021" = "Cidade Ademar",
                               "0022" = "Cidade Dutra",
                               "0023" = "Cidade Líder",
                               "0024" = "Cidade Tiradentes",
                               "0025" = "Consolação",
                               "0026" = "Cursino",
                               "0027" = "Ermelino Matarazzo",
                               "0028" = "Freguesia Do Ó",
                               "0029" = "Grajaú",
                               "0030" = "Guaianases",
                               "0031" = "Iguatemi",
                               "0032" = "Ipiranga",
                               "0033" = "Itaim Bibi",
                               "0034" = "Itaim Paulista",
                               "0035" = "Itaquera",
                               "0036" = "Jabaquara",
                               "0037" = "Jaçanã",
                               "0038" = "Jaguara",
                               "0039" = "Jaguaré",
                               "0040" = "Jaraguá",
                               "0041" = "Jardim Ângela",
                               "0042" = "Jardim Helena",
                               "0043" = "Jardim Paulista",
                               "0044" = "José Bonifácio",
                               "0045" = "Lajeado",
                               "0046" = "Lapa",
                               "0047" = "Liberdade",
                               "0048" = "Limão",
                               "0049" = "Mandaqui",
                               "0050" = "Marsilac",
                               "0051" = "Moema",
                               "0052" = "Mooca",
                               "0053" = "Morumbi",
                               "0054" = "Cachoeirinha",
                               "0055" = "Parelheiros",
                               "0056" = "Pari",
                               "0057" = "Parque Do Carmo",
                               "0058" = "Pedreira",
                               "0059" = "Penha",
                               "0060" = "Perdizes",
                               "0061" = "Perus",
                               "0062" = "Pinheiros",
                               "0063" = "Pirituba",
                               "0064" = "Ponte Rasa",
                               "0065" = "Raposo Tavares",
                               "0066" = "República",
                               "0067" = "Rio Pequeno",
                               "0068" = "Sacomã",
                               "0069" = "Santa Cecília",
                               "0070" = "Santana",
                               "0071" = "Santo Amaro",
                               "0072" = "São Domingos",
                               "0073" = "São Lucas",
                               "0074" = "Jardim São Luís",
                               "0075" = "São Mateus",
                               "0076" = "São Miguel",
                               "0077" = "São Rafael",
                               "0078" = "Sapopemba",
                               "0079" = "Saúde",
                               "0080" = "Sé",
                               "0081" = "Socorro",
                               "0082" = "Tatuapé",
                               "0083" = "Tremembé",
                               "0084" = "Tucuruvi",
                               "0085" = "Vila Andrade",
                               "0086" = "Vila Curuçá",
                               "0087" = "Vila Formosa",
                               "0088" = "Vila Jacuí",
                               "0089" = "Vila Leopoldina",
                               "0090" = "Vila Maria",
                               "0091" = "Vila Guilherme",
                               "0092" = "Vila Mariana",
                               "0093" = "Vila Matilde",
                               "0094" = "Vila Medeiros",
                               "0095" = "Vila Prudente",
                               "0096" = "Vila Sônia",
                               "9999" = "Outros Sp",
                               "{ñ class}" = ""),
         raca_cor = recode(raca_cor, 
                           "1" = "Indígena",
                           "2" = "Branca",
                           "4"  = "Preta",
                           "6"  = "Amarela",
                           "8"  = "Parda",
                           "9"  = "Não identificado",
                           "-1"  = "Ignorado"))
                           
# Criando tabela de remuneração média e número de vínculos
vinculos_distrito <- df %>% 
  group_by(distritos_sp) %>% 
  summarize(media_remuneracao = mean(valor_remuneracao_media),
            numero_vinculos = sum(vinculo_ativo_3112 == 1),
            numero_semvinculos = sum(vinculo_ativo_3112 == 0)) %>% 
  adorn_totals("row")
  
vinculos_distrito

# Criando tabela de vículos aivos em 31/12 por raça  
vinculos_raca = df %>% 
  filter(vinculo_ativo_3112 == 1) %>% 
  group_by(distritos_sp, raca_cor) %>% 
  summarise(vinculos_ativos = sum(vinculo_ativo_3112)) %>% 
  spread(raca_cor, vinculos_ativos) %>% 
  adorn_totals("row")

View(vinculos_raca)

# Tabela de vínculo ativo por faixa etária
    # Criando a categorização da faixa etária
breaksFaixa = c(0, 18, 24, 29, 34, 39, 44, 49, 54, 59, 64, 69, 74, Inf)
labelsFaixa = c("0 a 18", "18 a 24", "25 a 29", "30 a 34", "35 a 39", "40 a 44", "45 a 49", "50 a 54", "55 a 59", "60 a 64", "65 a 69", "70 a 74", "75 ou mais")
df<- df %>%
  mutate(faixa_smads = cut(df$idade, breaks = breaksFaixa, labels = labelsFaixa))

# Tabela dinâmica de vínculos por faixa etária
vinculos_faixaEtaria = df %>%
  filter(vinculo_ativo_3112 == 1) %>% 
  group_by(distritos_sp, faixa_smads) %>% 
  summarise(vinculos_ativos = sum(vinculo_ativo_3112)) %>% 
  spread(faixa_smads, vinculos_ativos) %>%
  adorn_totals("row")


View(vinculos_faixaEtaria)

# Extraindo tabelas para arquivo excel
write_xlsx(list(Renda = vinculos_distrito, Raça = vinculos_raca, Faixa_Etária = vinculos_faixaEtaria), "rais_vazios2.xlsx")

